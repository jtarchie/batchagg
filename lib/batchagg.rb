# frozen_string_literal: true

require "ostruct"
require "active_record"

module BatchAgg
  # Represents a single aggregate function definition
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

    def column_based?
      type == :column
    end

    def block?
      !block.nil?
    end
  end

  # Handles association traversal for correlated subqueries
  class AssociationResolver
    def initialize(base_model_class, outer_table_alias)
      @base_model_class = base_model_class
      @outer_table_alias = outer_table_alias
    end

    def resolve_association(association_name)
      reflection = find_association_reflection(association_name)
      target_class = reflection.klass
      correlation_condition = build_correlation_condition(reflection)

      target_class.where(correlation_condition)
    end

    def association?(association_name)
      @base_model_class.reflect_on_association(association_name).present?
    end

    private

    def find_association_reflection(association_name)
      reflection = @base_model_class.reflect_on_association(association_name)
      raise NoMethodError, "Association '#{association_name}' not found" unless reflection

      reflection
    end

    def build_correlation_condition(reflection)
      if belongs_to_association?(reflection)
        build_belongs_to_condition(reflection)
      else
        build_has_many_condition(reflection)
      end
    end

    def belongs_to_association?(reflection)
      reflection.macro == :belongs_to
    end

    def build_belongs_to_condition(reflection)
      target_table = reflection.klass.arel_table
      foreign_key_column = @outer_table_alias[reflection.foreign_key]
      primary_key_column = target_table[reflection.association_primary_key]

      primary_key_column.eq(foreign_key_column)
    end

    def build_has_many_condition(reflection)
      target_table = reflection.klass.arel_table
      foreign_key_column = target_table[reflection.foreign_key]
      primary_key_column = @outer_table_alias[reflection.active_record_primary_key]

      foreign_key_column.eq(primary_key_column)
    end
  end

  # Provides method_missing interface for association access in aggregate blocks
  class CorrelatedRelationBuilder
    def initialize(base_model_class, outer_table_alias)
      @association_resolver = AssociationResolver.new(base_model_class, outer_table_alias)
    end

    def method_missing(association_name, *args, &block_arg)
      validate_method_call(association_name, args, block_arg)
      @association_resolver.resolve_association(association_name)
    end

    def respond_to_missing?(method_name, include_private = false)
      @association_resolver.association?(method_name) || super
    end

    private

    def validate_method_call(association_name, args, block_arg)
      return unless args.any? || block_arg

      raise ArgumentError,
            "Unexpected arguments or block for association '#{association_name}' in aggregate definition."
    end
  end

  # Provides access to outer table attributes in aggregate blocks
  class OuterTableAttributeAccessor
    def initialize(outer_table_alias, base_model_class)
      @outer_table_alias = outer_table_alias
      @base_model_class = base_model_class
    end

    def method_missing(method_name, *args, &block_arg)
      return super unless valid_attribute_access?(method_name, args, block_arg)

      @outer_table_alias[method_name]
    end

    def respond_to_missing?(method_name, include_private = false)
      column?(method_name) || super
    end

    private

    def valid_attribute_access?(method_name, args, block_arg)
      args.empty? && block_arg.nil? && column?(method_name)
    end

    def column?(method_name)
      @base_model_class.columns_hash.key?(method_name.to_s)
    end
  end

  # Handles building SQL projections for column aggregates
  class ColumnProjectionBuilder
    def initialize(outer_table_alias, base_model_class)
      @outer_table_alias = outer_table_alias
      @attribute_accessor = OuterTableAttributeAccessor.new(outer_table_alias, base_model_class)
    end

    def build_projection(aggregate_def, correlation_builder)
      if aggregate_def.block?
        build_block_based_projection(aggregate_def, correlation_builder)
      else
        build_direct_attribute_projection(aggregate_def)
      end
    end

    private

    def build_direct_attribute_projection(aggregate_def)
      @outer_table_alias[aggregate_def.column].as(aggregate_def.name.to_s)
    end

    def build_block_based_projection(aggregate_def, correlation_builder)
      if (aliased_attribute_projection = try_aliased_attribute(aggregate_def))
        aliased_attribute_projection
      else
        build_subquery_projection(aggregate_def, correlation_builder)
      end
    end

    def try_aliased_attribute(aggregate_def)
      value = aggregate_def.block.call(@attribute_accessor)
      return nil unless arel_attribute_or_literal?(value)

      value.as(aggregate_def.name.to_s)
    rescue NoMethodError, ArgumentError
      nil
    end

    def arel_attribute_or_literal?(value)
      value.is_a?(Arel::Attributes::Attribute) || value.is_a?(Arel::Nodes::SqlLiteral)
    end

    def build_subquery_projection(aggregate_def, correlation_builder)
      relation = aggregate_def.block.call(correlation_builder)
      validate_subquery_relation(relation, aggregate_def.name)

      subquery_sql = relation.to_sql
      Arel.sql("(#{subquery_sql})").as(aggregate_def.name.to_s)
    rescue StandardError => e
      raise ArgumentError,
            "Block for column aggregate '#{aggregate_def.name}' failed. " \
            "Not a valid aliased attribute or subquery. Error: #{e.message}"
    end

    def validate_subquery_relation(relation, aggregate_name)
      return if relation.is_a?(ActiveRecord::Relation)

      raise ArgumentError,
            "Block for column subquery '#{aggregate_name}' must return an ActiveRecord::Relation. " \
            "Got: #{relation.class}"
    end
  end

  # Builds SQL for aggregate functions (count, sum, etc.)
  class AggregateSubqueryBuilder
    AGGREGATE_FUNCTIONS = {
      count: ->(relation) { relation.select(Arel.star.count) },
      count_distinct: ->(relation, column) { relation.select(relation.model.arel_table[column].count(true)) },
      sum: ->(relation, column) { relation.select(relation.model.arel_table[column].sum) },
      avg: ->(relation, column) { relation.select(relation.model.arel_table[column].average) },
      min: ->(relation, column) { relation.select(relation.model.arel_table[column].minimum) },
      max: ->(relation, column) { relation.select(relation.model.arel_table[column].maximum) }
    }.freeze

    def build_subquery_sql(relation, aggregate_def)
      base_query = relation.except(:select)

      case aggregate_def.type
      when :count
        build_count_query(base_query)
      when :count_expression
        build_count_expression_query(base_query, aggregate_def.expression)
      when :count_distinct
        build_count_distinct_query(base_query, aggregate_def.column)
      when :count_distinct_expression
        build_count_distinct_expression_query(base_query, aggregate_def.expression)
      when :sum
        build_sum_query(base_query, aggregate_def.column)
      when :sum_expression
        build_sum_expression_query(base_query, aggregate_def.expression)
      when :avg
        build_avg_query(base_query, aggregate_def.column)
      when :avg_expression
        build_avg_expression_query(base_query, aggregate_def.expression)
      when :min
        build_min_query(base_query, aggregate_def.column)
      when :min_expression
        build_min_expression_query(base_query, aggregate_def.expression)
      when :max
        build_max_query(base_query, aggregate_def.column)
      when :max_expression
        build_max_expression_query(base_query, aggregate_def.expression)
      when :string_agg
        build_string_agg_query(base_query, aggregate_def.column, aggregate_def.options)
      when :string_agg_expression
        build_string_agg_expression_query(base_query, aggregate_def.expression, aggregate_def.options)
      else
        raise ArgumentError, "Unsupported aggregate type: #{aggregate_def.type}"
      end
    end

    private

    def build_count_query(base_query)
      base_query.select(Arel.star.count).to_sql
    end

    def build_count_expression_query(base_query, expression)
      base_query.select(Arel.sql("COUNT(#{expression})")).to_sql
    end

    def build_count_distinct_query(base_query, column)
      table = base_query.model.arel_table
      base_query.select(table[column].count(true)).to_sql
    end

    def build_count_distinct_expression_query(base_query, expression)
      base_query.select(Arel.sql("COUNT(DISTINCT #{expression})")).to_sql
    end

    def build_sum_query(base_query, column)
      table = base_query.model.arel_table
      base_query.select(table[column].sum).to_sql
    end

    def build_sum_expression_query(base_query, expression)
      base_query.select(Arel.sql("SUM(#{expression})")).to_sql
    end

    def build_avg_query(base_query, column)
      table = base_query.model.arel_table
      base_query.select(table[column].average).to_sql
    end

    def build_avg_expression_query(base_query, expression)
      base_query.select(Arel.sql("AVG(#{expression})")).to_sql
    end

    def build_min_query(base_query, column)
      table = base_query.model.arel_table
      base_query.select(table[column].minimum).to_sql
    end

    def build_min_expression_query(base_query, expression)
      base_query.select(Arel.sql("MIN(#{expression})")).to_sql
    end

    def build_max_query(base_query, column)
      table = base_query.model.arel_table
      base_query.select(table[column].maximum).to_sql
    end

    def build_max_expression_query(base_query, expression)
      base_query.select(Arel.sql("MAX(#{expression})")).to_sql
    end

    def build_string_agg_query(base_query, column, options)
      table = base_query.model.arel_table
      delimiter = options&.dig(:delimiter)

      args = [table[column]]
      args << Arel::Nodes.build_quoted(delimiter) if delimiter

      base_query.select(Arel::Nodes::NamedFunction.new("GROUP_CONCAT", args)).to_sql
    end

    def build_string_agg_expression_query(base_query, expression, options)
      delimiter = options&.dig(:delimiter)

      args = [Arel.sql(expression)]
      args << Arel::Nodes.build_quoted(delimiter) if delimiter

      base_query.select(Arel::Nodes::NamedFunction.new("GROUP_CONCAT", args)).to_sql
    end
  end

  # Handles applying WHERE conditions from scope to outer table alias
  class ScopeConditionApplier
    def initialize(base_model, outer_table_alias)
      @base_model = base_model
      @outer_table_alias = outer_table_alias
    end

    def apply_conditions_to_query(arel_query, scope)
      processed_columns = apply_simple_where_conditions(arel_query, scope)
      apply_arel_where_conditions(arel_query, scope, processed_columns)
    end

    private

    def apply_simple_where_conditions(arel_query, scope)
      processed_columns = []

      return processed_columns unless scope.respond_to?(:where_values_hash)
      return processed_columns unless scope.where_values_hash.is_a?(Hash)

      scope.where_values_hash.each do |column_name, value|
        column_name_str = column_name.to_s

        if model_column?(column_name_str)
          arel_query.where(@outer_table_alias[column_name.to_sym].eq(value))
          processed_columns << column_name_str
        end
      end

      processed_columns
    end

    def apply_arel_where_conditions(arel_query, scope, processed_columns)
      where_clauses = extract_where_clauses(scope)

      where_clauses.each do |constraint|
        next unless simple_table_constraint?(constraint, processed_columns)

        apply_constraint_to_query(arel_query, constraint)
      end
    end

    def extract_where_clauses(scope)
      scope.arel.ast.cores.flat_map(&:wheres)
    end

    def simple_table_constraint?(constraint, processed_columns)
      return false unless constraint.left.is_a?(Arel::Attributes::Attribute)
      return false unless constraint.left.relation.name == @base_model.table_name
      return false if processed_columns.include?(constraint.left.name.to_s)

      true
    end

    def apply_constraint_to_query(arel_query, constraint)
      if constraint.is_a?(Arel::Nodes::Equality)
        apply_equality_constraint(arel_query, constraint)
      elsif constraint.is_a?(Arel::Nodes::In)
        apply_in_constraint(arel_query, constraint)
      end
    end

    def apply_equality_constraint(arel_query, constraint)
      column_name = constraint.left.name
      value = constraint.right
      arel_query.where(@outer_table_alias[column_name].eq(value))
    end

    def apply_in_constraint(arel_query, constraint)
      column_name = constraint.left.name
      values = extract_in_clause_values(constraint.right)
      arel_query.where(@outer_table_alias[column_name].in(values))
    end

    def extract_in_clause_values(right_operand)
      return right_operand unless right_operand.is_a?(Array)

      right_operand.map do |value|
        value.is_a?(Arel::Nodes::BindParam) ? value.value.value_before_type_cast : value
      end
    end

    def model_column?(column_name)
      @base_model.columns_hash.key?(column_name)
    end
  end

  # Main query builder that orchestrates the SQL generation
  class QueryBuilder
    def initialize(base_model)
      @base_model = base_model
      @subquery_builder = AggregateSubqueryBuilder.new
    end

    def build_query_for_scope(scope, aggregates)
      outer_table_alias = create_outer_table_alias
      builders = create_helper_builders(outer_table_alias)

      arel_query = create_base_query(scope, outer_table_alias)
      add_projections_to_query(arel_query, aggregates, builders, outer_table_alias)
      apply_scope_conditions(arel_query, scope, outer_table_alias)

      arel_query
    end

    private

    def create_outer_table_alias
      @base_model.arel_table.alias("batchagg_outer_#{@base_model.table_name}")
    end

    def create_helper_builders(outer_table_alias)
      {
        correlation: CorrelatedRelationBuilder.new(@base_model, outer_table_alias),
        column_projection: ColumnProjectionBuilder.new(outer_table_alias, @base_model)
      }
    end

    def create_base_query(scope, outer_table_alias)
      arel_query = Arel::SelectManager.new(scope.klass.connection)
      arel_query.from(outer_table_alias)

      primary_key_projection = outer_table_alias[@base_model.primary_key].as(@base_model.primary_key.to_s)
      arel_query.project(primary_key_projection)

      arel_query
    end

    def add_projections_to_query(arel_query, aggregates, builders, _outer_table_alias)
      aggregates.each do |aggregate_def|
        projection = build_projection_for_aggregate(aggregate_def, builders)
        arel_query.project(projection)
      end
    end

    def build_projection_for_aggregate(aggregate_def, builders)
      if aggregate_def.column_based?
        builders[:column_projection].build_projection(aggregate_def, builders[:correlation])
      else
        build_aggregate_function_projection(aggregate_def, builders[:correlation])
      end
    end

    def build_aggregate_function_projection(aggregate_def, correlation_builder)
      correlated_relation = aggregate_def.block.call(correlation_builder)
      subquery_sql = @subquery_builder.build_subquery_sql(correlated_relation, aggregate_def)
      Arel.sql("(#{subquery_sql})").as(aggregate_def.name.to_s)
    end

    def apply_scope_conditions(arel_query, scope, outer_table_alias)
      condition_applier = ScopeConditionApplier.new(@base_model, outer_table_alias)
      condition_applier.apply_conditions_to_query(arel_query, scope)
    end
  end

  # Handles type casting of primary key values for hash keys
  class PrimaryKeyTypeConverter
    def self.cast_for_hash_key(id_value, model_class)
      primary_key_column = find_primary_key_column(model_class)
      type_converter = create_type_converter(primary_key_column)
      type_converter.deserialize(id_value)
    end

    def self.find_primary_key_column(model_class)
      model_class.columns_hash[model_class.primary_key]
    end

    def self.create_type_converter(primary_key_column)
      ActiveRecord::Type.lookup(
        primary_key_column.type,
        limit: primary_key_column.limit,
        precision: primary_key_column.precision,
        scale: primary_key_column.scale
      )
    end
  end

  # Represents aggregate results for a single record
  class AggregateResultForRecord
    def initialize(row_data, aggregate_definitions)
      @data = symbolize_keys(row_data)
      define_aggregate_methods(aggregate_definitions)
    end

    private

    def symbolize_keys(hash)
      hash.transform_keys(&:to_sym)
    end

    def define_aggregate_methods(aggregate_definitions)
      aggregate_definitions.each do |aggregate_def|
        define_singleton_method(aggregate_def.name) do
          @data[aggregate_def.name]
        end
      end
    end
  end

  # Orchestrates query execution and result processing
  class AggregateResultProcessor
    def initialize(aggregates, base_model)
      @aggregates = aggregates
      @base_model = base_model
      @query_builder = QueryBuilder.new(base_model)
    end

    def process_single_record(record)
      scope = create_single_record_scope(record)
      process_scope(scope)
    end

    def process_scope(scope)
      query_results = execute_query(scope)
      convert_results_to_hash(query_results, scope.klass)
    end

    private

    def create_single_record_scope(record)
      @base_model.where(@base_model.primary_key => record.id)
    end

    def execute_query(scope)
      query_arel = @query_builder.build_query_for_scope(scope, @aggregates)
      scope.klass.connection.select_all(query_arel.to_sql).to_a
    end

    def convert_results_to_hash(query_results, model_class)
      query_results.each_with_object({}) do |row_hash, result_hash|
        record_id = extract_and_cast_record_id(row_hash, model_class)
        result_hash[record_id] = AggregateResultForRecord.new(row_hash, @aggregates)
      end
    end

    def extract_and_cast_record_id(row_hash, model_class)
      raw_id = row_hash[@base_model.primary_key.to_s]
      PrimaryKeyTypeConverter.cast_for_hash_key(raw_id, model_class)
    end
  end

  # Main class that provides the public API for aggregate results
  class AggregateResultClass
    def initialize(aggregates, base_model)
      @processor = AggregateResultProcessor.new(aggregates, base_model)
    end

    def only(record)
      @processor.process_single_record(record)
    end

    def from(scope)
      @processor.process_scope(scope)
    end
  end

  # Builder for creating aggregate definitions using DSL methods
  class AggregateDefinitionCollector
    def initialize
      @aggregates = []
    end

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

    attr_reader :aggregates

    private

    def add_aggregate(type, name, block, column: nil, expression: nil, options: nil)
      @aggregates << AggregateDefinition.new(name, type, block, column, expression, options)
    end
  end

  # Main builder that coordinates the DSL and creates the result class
  class AggregateBuilder
    def initialize(base_model)
      @base_model = base_model
    end

    def build_class(&block)
      collector = AggregateDefinitionCollector.new
      collector.instance_eval(&block)
      aggregates = collector.aggregates

      AggregateResultClass.new(aggregates, @base_model)
    end
  end

  # Public DSL module
  module DSL
    def aggregate(base_model, &block)
      builder = BatchAgg::AggregateBuilder.new(base_model)
      builder.build_class(&block)
    end
  end
end
