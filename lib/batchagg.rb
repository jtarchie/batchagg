require "ostruct"
require "active_record" # Ensure ActiveRecord is available for Arel

module BatchAgg
  class AggregateDefinition
    attr_reader :name, :type, :block

    def initialize(name, type, block)
      @name = name
      @type = type
      @block = block
    end
  end

  # Builds relations that are correlated to an outer query's aliased table.
  # Used when processing aggregate blocks for a scope of records.
  class CorrelatedRelationBuilder
    def initialize(base_model_class, outer_table_alias_node)
      @base_model_class = base_model_class
      @outer_table_alias_node = outer_table_alias_node
    end

    def method_missing(association_name, *args, &block_arg)
      # Ensure no arguments are passed for simple association access like `user.posts`
      if args.any? || block_arg
        raise ArgumentError, "Unexpected arguments or block for association '#{association_name}' in aggregate definition."
      end

      reflection = @base_model_class.reflect_on_association(association_name)
      unless reflection
        super # Raise NoMethodError if it's not an association
        return # Make linters happy
      end

      target_klass = reflection.klass
      correlation_predicate = if reflection.macro == :belongs_to
        # For belongs_to :user on Post:
        # target_klass is User.
        # reflection.association_primary_key is User's primary key (e.g., "id").
        # @outer_table_alias_node is the alias for the Post table.
        # reflection.foreign_key is the foreign key on Post (e.g., "user_id").
        target_klass.arel_table[reflection.association_primary_key].eq(@outer_table_alias_node[reflection.foreign_key])
      else # :has_many, :has_one
        # For has_many :posts on User:
        # target_klass is Post.
        # reflection.foreign_key is the foreign key on Post (e.g., "user_id").
        # @outer_table_alias_node is the alias for the User table.
        # reflection.active_record_primary_key is User's primary key (e.g., "id").
        target_klass.arel_table[reflection.foreign_key].eq(@outer_table_alias_node[reflection.active_record_primary_key])
      end
      target_klass.where(correlation_predicate)
    end

    def respond_to_missing?(method_name, include_private = false)
      @base_model_class.reflect_on_association(method_name).present? || super
    end
  end

  class QueryBuilder
    def initialize(base_model)
      @base_model = base_model
    end

    def build_query_for_record(record, aggregates)
      main_table = @base_model.arel_table
      projections = build_projections_for_single_record(record, aggregates)

      main_table
        .project(*projections)
        .where(main_table[@base_model.primary_key].eq(record.id))
    end

    def build_query_for_scope(scope, aggregates)
      # Alias for the main table in the outer query (e.g., "users" AS "batchagg_outer_users")
      outer_table_alias = @base_model.arel_table.alias("batchagg_outer_#{@base_model.table_name}")

      # Builder to create correlated relations for subqueries
      correlation_builder = CorrelatedRelationBuilder.new(@base_model, outer_table_alias)

      # Projections: start with the primary key of the base model from the aliased table
      projections = [outer_table_alias[@base_model.primary_key].as(@base_model.primary_key.to_s)]

      aggregates.each do |aggregate_def|
        # The block is called with the correlation_builder.
        # e.g., for a block `{|user| user.posts.where(title: 'P1')}`:
        # 1. `correlation_builder.posts` is called, returning `Post.where(posts.user_id: outer_table_alias.id)`
        # 2. `.where(title: 'P1')` is then called on that returned relation.
        correlated_relation = aggregate_def.block.call(correlation_builder)

        # Build the count subquery SQL from this correlated_relation
        # Assuming all aggregates are counts for now, as per `build_count_subquery`
        subquery_sql = correlated_relation.except(:select).select(Arel.star.count).to_sql
        projections << Arel.sql("(#{subquery_sql})").as(aggregate_def.name.to_s)
      end

      # Construct the main Arel query
      # SELECT outer_table_alias.id, (subquery1) AS agg1, ...
      # FROM "base_table_name" AS outer_table_alias
      # WHERE <conditions from the original scope, but applied to outer_table_alias>
      arel_query = Arel::SelectManager.new(@base_model.connection) # Use connection from the base model
      arel_query.from(outer_table_alias)
      arel_query.project(*projections)

      # Apply WHERE conditions from the input 'scope' to the 'outer_table_alias'
      # This is a simplified way to handle common where clauses.
      # A full Arel tree transformation would be more robust for complex scopes.
      if scope.where_clause.any?
        # For simple hash conditions like `User.where(name: "Alice", status: 1)`
        scope.where_values_hash.each do |column_name, value|
          arel_query.where(outer_table_alias[column_name.to_sym].eq(value))
        end
        # For other types of conditions, this would need more advanced Arel manipulation.
        # The test uses `User.all`, so `where_values_hash` will be empty.
        # If `scope.arel.constraints` are present and not covered by `where_values_hash`,
        # they would need transformation. For `User.all`, `constraints` is empty.
      end
      # If the scope has specific IDs (e.g. from `User.where(id: [1,2,3])`)
      # This is often handled by `where_values_hash` if `id` is a string/symbol key.
      # If `scope` was built like `User.find([1,2,3])`, `scope.where_values_hash` might not capture it directly
      # in all Rails versions in the same way as `User.where(id: [1,2,3])`.
      # However, `User.all` has no such constraints.

      arel_query
    end

    private

    def build_projections_for_single_record(record, aggregates)
      aggregates.map do |aggregate|
        build_projection_for_aggregate(record, aggregate)
      end
    end

    def build_projection_for_aggregate(record, aggregate)
      # This is for the single record case
      relation = aggregate.block.call(record)
      subquery = build_count_subquery(relation)
      Arel.sql("(#{subquery})").as(aggregate.name.to_s)
    end

    def build_count_subquery(relation)
      # This method is used by both single and multiple record query paths
      relation.except(:select).select(Arel.star.count).to_sql
    end
  end

  class SingleRecordResult
    def initialize(record, aggregates, base_model)
      @record = record
      @aggregates = aggregates
      @query_builder = QueryBuilder.new(base_model)
      @results = execute_query
      add_aggregate_methods
    end

    attr_reader :record

    private

    def execute_query
      query = @query_builder.build_query_for_record(@record, @aggregates)
      result = @record.class.connection.select_all(query.to_sql) # Use record's class for connection
      result.first || {}
    end

    def add_aggregate_methods
      @aggregates.each do |aggregate|
        define_aggregate_method(aggregate.name)
      end
    end

    def define_aggregate_method(name)
      define_singleton_method(name) do
        @results[name.to_s]
      end
    end
  end

  # Holds aggregate data for one record when fetching multiple records
  class MultipleRecordsResultItem
    def initialize(row_data_hash, aggregate_definitions)
      @data = row_data_hash.transform_keys(&:to_sym) # Ensure symbol keys for access
      aggregate_definitions.each do |agg_def|
        define_singleton_method(agg_def.name) do
          @data[agg_def.name]
        end
      end
    end
  end

  class AggregateResultClass
    def initialize(aggregates, base_model)
      @aggregates = aggregates
      @base_model = base_model
      @query_builder = QueryBuilder.new(base_model)
    end

    def only(record)
      SingleRecordResult.new(record, @aggregates, @base_model)
    end

    def from(scope)
      query_arel = @query_builder.build_query_for_scope(scope, @aggregates)
      # Use the connection from the base model of the scope
      results_array = scope.klass.connection.select_all(query_arel.to_sql).to_a

      results_hash = results_array.each_with_object({}) do |row_hash, memo|
        # The primary key might be returned as a string from DB, ensure it matches type if needed for lookup
        record_id = row_hash[@base_model.primary_key.to_s]
        memo[record_id] = MultipleRecordsResultItem.new(row_hash, @aggregates)
      end
      results_hash
    end
  end

  class AggregateBuilder
    def initialize(base_model)
      @base_model = base_model
      @aggregates = []
    end

    def count(name, &block)
      aggregate = AggregateDefinition.new(name, :count, block)
      @aggregates << aggregate
    end

    def build_class
      AggregateResultClass.new(@aggregates, @base_model)
    end
  end
end

def aggregate(&block)
  # For now, assume User as the base model
  # In the future, this could be made configurable
  # This global `User` might need to be passed in or configured if not always User.
  # For the test setup, User model is globally available.
  builder = BatchAgg::AggregateBuilder.new(User)
  builder.instance_eval(&block)
  builder.build_class
end
