# frozen_string_literal: true

require "ostruct"
require "active_record" # Ensure ActiveRecord is available for Arel

module BatchAgg
  class AggregateDefinition
    attr_reader :name, :type, :block, :column, :expression, :options

    def initialize(name, type, block, column = nil, expression = nil, options = nil)
      @name = name
      @type = type
      @block = block
      @column = column
      @expression = expression
      @options = options
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
        raise ArgumentError,
              "Unexpected arguments or block for association '#{association_name}' in aggregate definition."
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

  # Helper class to access attributes on the outer table alias
  class OuterTableAttributeAccessor
    def initialize(outer_table_alias_node, base_model_class)
      @outer_table_alias_node = outer_table_alias_node
      @base_model_class = base_model_class
    end

    def method_missing(method_name, *args, &block_arg)
      if args.empty? && block_arg.nil? && @base_model_class.columns_hash.key?(method_name.to_s)
        @outer_table_alias_node[method_name]
      else
        super
      end
    end

    def respond_to_missing?(method_name, include_private = false)
      @base_model_class.columns_hash.key?(method_name.to_s) || super
    end
  end

  class QueryBuilder
    def initialize(base_model)
      @base_model = base_model
    end

    def build_query_for_scope(scope, aggregates)
      outer_table_alias = @base_model.arel_table.alias("batchagg_outer_#{@base_model.table_name}")
      correlation_builder = CorrelatedRelationBuilder.new(@base_model, outer_table_alias)
      outer_table_accessor = OuterTableAttributeAccessor.new(outer_table_alias, @base_model)
      projections = [outer_table_alias[@base_model.primary_key].as(@base_model.primary_key.to_s)]

      aggregates.each do |aggregate_def|
        if aggregate_def.type == :column
          if aggregate_def.block.nil?
            # Case 1: Direct attribute from base model (e.g., column(:age))
            projections << outer_table_alias[aggregate_def.column].as(aggregate_def.name.to_s)
          else
            # Block is present. This could be:
            # Case 2: Aliased attribute from base model (e.g., column(:aliased_aged, &:age))
            # Case 3: Subquery (e.g., column(:first_post_views) { |s| s.posts.limit(1).select(:views) })

            is_aliased_attribute_case = false
            begin
              # Attempt to treat as Case 2: Aliased attribute
              value = aggregate_def.block.call(outer_table_accessor)
              if value.is_a?(Arel::Attributes::Attribute) || value.is_a?(Arel::Nodes::SqlLiteral)
                projections << value.as(aggregate_def.name.to_s)
                is_aliased_attribute_case = true
              end
              # If `value` is something else (e.g., an ActiveRecord::Relation by mistake, or a non-Arel literal),
              # it's not a valid aliased attribute projection, so is_aliased_attribute_case remains false.
            rescue NoMethodError, ArgumentError
              # If calling with outer_table_accessor fails (e.g., block tried `scope.posts`,
              # or arity mismatch), it's likely Case 3 (subquery).
              # is_aliased_attribute_case remains false.
            end

            unless is_aliased_attribute_case
              # Fallback to Case 3: Treat as a subquery
              begin
                correlated_relation = aggregate_def.block.call(correlation_builder)
                raise ArgumentError, "Block for column subquery '#{aggregate_def.name}' must return an ActiveRecord::Relation. Got: #{correlated_relation.class}" unless correlated_relation.is_a?(ActiveRecord::Relation)

                # The user is responsible for ensuring this relation results in a scalar subquery
                # (e.g., by using .select() and .limit(1)).
                subquery_sql = correlated_relation.to_sql
                projections << Arel.sql("(#{subquery_sql})").as(aggregate_def.name.to_s)
              rescue StandardError => e
                raise ArgumentError, "Block for column aggregate '#{aggregate_def.name}' is ambiguous or malformed. It did not resolve as an aliased attribute and failed as a subquery. Original error: #{e.message}"
              end
            end
          end
        else
          # Handles other aggregate types (sum, count, etc.)
          correlated_relation = aggregate_def.block.call(correlation_builder)
          subquery_sql = build_subquery_for_aggregate(correlated_relation, aggregate_def)
          projections << Arel.sql("(#{subquery_sql})").as(aggregate_def.name.to_s)
        end
      end

      arel_query = Arel::SelectManager.new(scope.klass.connection) # Use connection from scope's class
      arel_query.from(outer_table_alias)
      arel_query.project(*projections)

      # Apply WHERE conditions from the input scope to the outer_table_alias.
      # This primarily uses where_values_hash for simple equality conditions.
      # More complex conditions in the scope (e.g., involving OR, custom SQL strings, joins on other tables)
      # might not be fully translated. A comprehensive solution would require
      # an Arel visitor to parse and rewrite all conditions from scope.arel.constraints
      # against outer_table_alias. This is a known complex problem.
      processed_keys_from_where_values = []
      if scope.respond_to?(:where_values_hash) && scope.where_values_hash.is_a?(Hash)
        scope.where_values_hash.each do |column_name, value|
          column_name_str = column_name.to_s
          if @base_model.columns_hash.key?(column_name_str)
            arel_query.where(outer_table_alias[column_name.to_sym].eq(value))
            processed_keys_from_where_values << column_name_str
          end
        end
      end

      # Attempt to handle other simple conditions from arel.constraints not covered by where_values_hash
      # This focuses on conditions directly on the base model's table.
      where_clauses = scope.arel.ast.cores.flat_map(&:wheres) # Get all where clauses from all cores
      where_clauses.each do |constraint|
        next unless constraint.left.is_a?(Arel::Attributes::Attribute) &&
                    constraint.left.relation.name == @base_model.table_name &&
                    !processed_keys_from_where_values.include?(constraint.left.name.to_s)

        if constraint.is_a?(Arel::Nodes::Equality)
          arel_query.where(outer_table_alias[constraint.left.name].eq(constraint.right))
        elsif constraint.is_a?(Arel::Nodes::In)
          # Ensure values for IN clause are literals if they are BindParam nodes
          values = if constraint.right.is_a?(Array)
                     constraint.right.map do |v|
                       v.is_a?(Arel::Nodes::BindParam) ? v.value.value_before_type_cast : v
                     end
                   else
                     constraint.right
                   end
          arel_query.where(outer_table_alias[constraint.left.name].in(values))
          # Add more handlers for other Arel node types (e.g., NotEqual, GreaterThan) if deemed necessary
          # and can be simply translated.
        end
      end

      arel_query
    end

    private

    def build_subquery_for_aggregate(relation, aggregate_def)
      # `relation` is an ActiveRecord_Relation (e.g., Post.where(...))
      # `relation.model.arel_table` gives the Arel::Table for that relation (e.g., posts table)
      subject_table = relation.model.arel_table
      base_query = relation.except(:select) # Start with the correlated relation

      case aggregate_def.type
      when :count
        base_query.select(Arel.star.count).to_sql
      when :count_expression
        base_query.select(Arel.sql("COUNT(#{aggregate_def.expression})")).to_sql
      when :count_distinct
        base_query.select(subject_table[aggregate_def.column].count(true)).to_sql # true for DISTINCT
      when :count_distinct_expression
        base_query.select(Arel.sql("COUNT(DISTINCT #{aggregate_def.expression})")).to_sql
      when :sum
        base_query.select(subject_table[aggregate_def.column].sum).to_sql
      when :sum_expression
        base_query.select(Arel.sql("SUM(#{aggregate_def.expression})")).to_sql
      when :avg
        base_query.select(subject_table[aggregate_def.column].average).to_sql
      when :avg_expression
        base_query.select(Arel.sql("AVG(#{aggregate_def.expression})")).to_sql
      when :min
        base_query.select(subject_table[aggregate_def.column].minimum).to_sql
      when :min_expression
        base_query.select(Arel.sql("MIN(#{aggregate_def.expression})")).to_sql
      when :max
        base_query.select(subject_table[aggregate_def.column].maximum).to_sql
      when :max_expression
        base_query.select(Arel.sql("MAX(#{aggregate_def.expression})")).to_sql
      when :string_agg # New case
        delimiter = aggregate_def.options&.dig(:delimiter) # Safely access delimiter

        # For SQLite, GROUP_CONCAT(expr, delimiter) or GROUP_CONCAT(expr)
        # Arel doesn't have a direct helper for GROUP_CONCAT's optional second arg in a cross-db way.
        # We build it using Arel::Nodes::NamedFunction.
        args = [subject_table[aggregate_def.column]]
        args << Arel::Nodes.build_quoted(delimiter) if delimiter

        base_query.select(Arel::Nodes::NamedFunction.new("GROUP_CONCAT", args)).to_sql
      when :string_agg_expression
        delimiter = aggregate_def.options&.dig(:delimiter)

        args = [Arel.sql(aggregate_def.expression)]
        args << Arel::Nodes.build_quoted(delimiter) if delimiter

        base_query.select(Arel::Nodes::NamedFunction.new("GROUP_CONCAT", args)).to_sql
      else
        raise ArgumentError, "Unsupported aggregate type: #{aggregate_def.type}"
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
      scope = @base_model.where(@base_model.primary_key => record.id)
      from(scope)
    end

    def from(scope)
      query_arel = @query_builder.build_query_for_scope(scope, @aggregates)
      results_array = scope.klass.connection.select_all(query_arel.to_sql).to_a

      results_array.each_with_object({}) do |row_hash, memo|
        record_id_from_db = row_hash[@base_model.primary_key.to_s]
        casted_record_id = cast_id_for_hash_key(record_id_from_db, scope.klass)
        memo[casted_record_id] = MultipleRecordsResultItem.new(row_hash, @aggregates)
      end
    end

    private

    def cast_id_for_hash_key(id_value, model_class)
      # Ensure the ID used as a hash key is of the expected type,
      # matching how record.id would behave.
      pk_column = model_class.columns_hash[model_class.primary_key]
      # Use ActiveRecord's type casting for the primary key.
      # This handles integers, UUIDs, etc., correctly.
      # pk_column.type returns the type symbol (e.g., :integer, :string)
      # pk_column.limit, pk_column.precision, pk_column.scale provide the necessary options
      type = ActiveRecord::Type.lookup(
        pk_column.type,
        limit: pk_column.limit,
        precision: pk_column.precision,
        scale: pk_column.scale
      )
      type.deserialize(id_value)
    end
  end

  class AggregateBuilder
    def initialize(base_model)
      @base_model = base_model
      @aggregates = []
    end

    private

    def add_aggregate(type, name, block, column: nil, expression: nil, options: nil)
      @aggregates << AggregateDefinition.new(name, type, block, column, expression, options)
    end

    public

    def column(name, &block)
      add_aggregate(:column, name, block, column: name)
    end

    def count(name, &block)
      add_aggregate(:count, name, block)
    end

    def count_distinct(name, column, &block)
      add_aggregate(:count_distinct, name, block, column: column)
    end

    def count_expression(name, expression, &block)
      add_aggregate(:count_expression, name, block, expression: expression)
    end

    def count_distinct_expression(name, expression, &block)
      add_aggregate(:count_distinct_expression, name, block, expression: expression)
    end

    def sum(name, column, &block)
      add_aggregate(:sum, name, block, column: column)
    end

    def sum_expression(name, expression, &block)
      add_aggregate(:sum_expression, name, block, expression: expression)
    end

    def avg(name, column, &block)
      add_aggregate(:avg, name, block, column: column)
    end

    def avg_expression(name, expression, &block)
      add_aggregate(:avg_expression, name, block, expression: expression)
    end

    def min(name, column, &block)
      add_aggregate(:min, name, block, column: column)
    end

    def min_expression(name, expression, &block)
      add_aggregate(:min_expression, name, block, expression: expression)
    end

    def max(name, column, &block)
      add_aggregate(:max, name, block, column: column)
    end

    def max_expression(name, expression, &block)
      add_aggregate(:max_expression, name, block, expression: expression)
    end

    def string_agg(name, column, delimiter: nil, &block)
      add_aggregate(:string_agg, name, block, column: column, options: { delimiter: delimiter })
    end

    def string_agg_expression(name, expression, delimiter: nil, &block)
      add_aggregate(:string_agg_expression, name, block, expression: expression, options: { delimiter: delimiter })
    end

    def build_class
      AggregateResultClass.new(@aggregates, @base_model)
    end
  end

  module DSL
    def aggregate(base_model, &block)
      builder = BatchAgg::AggregateBuilder.new(base_model)
      builder.instance_eval(&block)
      builder.build_class
    end
  end
end
