# frozen_string_literal: true

require "active_record"

module BatchAgg
  AggregateDef = Struct.new(:name, :type, :block, :column, :expression, :options, keyword_init: true) do
    def column_based? = type == :column
    def computed? = type == :computed
    def custom? = type == :custom
    def block? = !block.nil?
  end

  def self.call_with_optional_kwargs(block, receiver, **)
    return receiver unless block

    has_kwargs = block.parameters.any? { |type, _| %i[key keyreq keyrest].include?(type) }
    has_kwargs ? block.call(receiver, **) : block.call(receiver)
  end

  # Helper for building association queries
  module AssociationQueryBuilder
    private

    def build_query(reflection, join_builder)
      reflection.klass
      if reflection.through_reflection
        build_through_query(reflection, join_builder)
      else
        build_direct_query(reflection, join_builder)
      end
    end

    def build_direct_query(reflection, join_builder)
      target = reflection.klass
      target_table = target.arel_table
      join_condition = if reflection.macro == :belongs_to
                         join_builder.build(target_table[reflection.association_primary_key], reflection.foreign_key)
                       else # has_many, has_one
                         join_builder.build(target_table[reflection.foreign_key], reflection.active_record_primary_key)
                       end
      target.where(join_condition)
    end

    def build_through_query(reflection, join_builder)
      target = reflection.klass
      through = reflection.through_reflection
      source = reflection.source_reflection
      final_target_table = target.arel_table
      through_table = through.klass.arel_table

      cond1 = if source.macro == :belongs_to
                through_table[source.foreign_key].eq(final_target_table[source.association_primary_key])
              else
                final_target_table[source.foreign_key].eq(through_table[source.active_record_primary_key])
              end

      cond2 = if through.macro == :belongs_to
                join_builder.build(through_table[through.association_primary_key], through.foreign_key)
              else
                join_builder.build(through_table[through.foreign_key], through.active_record_primary_key)
              end

      subquery = through_table.project(Arel.sql("1")).where(cond1.and(cond2))
      target.where(subquery.exists)
    end
  end

  # Join builder for subquery IN (...)
  class SubqueryJoinBuilder
    def initialize(scope)
      @scope = scope
    end

    def build(left, right_key)
      left.in(@scope.select(right_key).arel)
    end
  end

  # Join builder for correlated subqueries
  class CorrelatedJoinBuilder
    def initialize(outer_table)
      @outer_table = outer_table
    end

    def build(left, right_key)
      left.eq(@outer_table[right_key])
    end
  end

  # Join builder for CTE
  class CteJoinBuilder
    def initialize(cte_table)
      @cte_table = cte_table
    end

    def build(left, right_key)
      subquery = @cte_table.project(@cte_table[right_key])
      left.in(subquery)
    end
  end

  class CombinedAssocMagic
    include AssociationQueryBuilder

    def initialize(scope, join_builder_scope: nil)
      @scope = scope
      @model = scope.model
      @join_builder = SubqueryJoinBuilder.new(join_builder_scope || scope)
    end

    def method_missing(name, *, &)
      reflection = @model.reflect_on_association(name)
      return build_query(reflection, @join_builder) if reflection

      if @scope.respond_to?(name)
        @scope.public_send(name, *, &)
      else
        @model.public_send(name, *, &)
      end
    end

    def respond_to_missing?(name, include_private = false)
      @model.reflect_on_association(name) || @scope.respond_to?(name, include_private) || @model.respond_to?(name, include_private) || super
    end
  end

  class CteAssocMagic
    include AssociationQueryBuilder

    def initialize(model, cte_table)
      @model = model
      @cte_table = cte_table
      @aliased_cte_table = cte_table.alias(model.table_name)
      @join_builder = CteJoinBuilder.new(cte_table)
    end

    def method_missing(name, *, &)
      reflection = @model.reflect_on_association(name)
      return build_query(reflection, @join_builder) if reflection

      @model.from(@aliased_cte_table).public_send(name, *, &)
    end

    def respond_to_missing?(name, include_private = false)
      @model.reflect_on_association(name) || @model.respond_to?(name, include_private) || super
    end
  end

  class AssocMagic
    include AssociationQueryBuilder

    def initialize(model, outer_table)
      @model = model
      @outer_table = outer_table
      @join_builder = CorrelatedJoinBuilder.new(@outer_table)
    end

    def method_missing(name, *, &)
      reflection = @model.reflect_on_association(name)
      return build_query(reflection, @join_builder) if reflection

      @model.public_send(name, *, &)
    end

    def respond_to_missing?(name, *)
      @model.reflect_on_association(name) || @model.respond_to?(name) || super
    end
  end

  class AttrMagic
    def initialize(model, outer_table)
      @model = model
      @outer_table = outer_table
    end

    def method_missing(name, *args, &)
      if @model.columns_hash.key?(name.to_s)
        @outer_table[name]
      else
        super
      end
    end

    def respond_to_missing?(name, *)
      @model.columns_hash.key?(name.to_s) || super
    end
  end

  class ColumnProj
    def initialize(outer_table, model)
      @outer_table = outer_table
      @attr_magic = AttrMagic.new(model, outer_table)
    end

    def build(defn, corr, **)
      if defn.block?
        val = begin
          BatchAgg.call_with_optional_kwargs(defn.block, @attr_magic, **)
        rescue StandardError
          nil
        end
        return val.as(defn.name.to_s) if val.is_a?(Arel::Attributes::Attribute) || val.is_a?(Arel::Nodes::SqlLiteral)

        rel = BatchAgg.call_with_optional_kwargs(defn.block, corr, **)
        Arel.sql("(#{rel.to_sql})").as(defn.name.to_s)
      else
        @outer_table[defn.column].as(defn.name.to_s)
      end
    end
  end

  module Drivers
    def self.for(connection)
      adapter_name = connection.adapter_name.downcase
      if adapter_name.include?("mysql")
        Mysql.new
      elsif adapter_name.include?("postgresql")
        Postgres.new
      else
        Sqlite.new
      end
    end

    class Base
      def string_agg(query, column, delimiter)
        raise NotImplementedError
      end

      def string_agg_expression(query, expression, delimiter)
        raise NotImplementedError
      end

      def cast_to_string(expr)
        expr # Default: no cast
      end

      def normalize_result(row)
        row # Default: no normalization
      end

      def supports_cte?
        true # Assume modern DBs support CTEs by default
      end
    end

    class Sqlite < Base
      def string_agg(query, column, delimiter)
        args = [column]
        args << Arel::Nodes.build_quoted(delimiter) if delimiter
        query.select(Arel::Nodes::NamedFunction.new("GROUP_CONCAT", args))
      end

      def string_agg_expression(query, expression, delimiter)
        args = [Arel.sql(expression)]
        args << Arel::Nodes.build_quoted(delimiter) if delimiter
        query.select(Arel::Nodes::NamedFunction.new("GROUP_CONCAT", args))
      end

      def cast_to_string(expr)
        "CAST(#{expr} AS TEXT)"
      end
    end

    class Postgres < Base
      def string_agg(query, column, delimiter)
        args = [column]
        args << Arel::Nodes.build_quoted(delimiter) if delimiter
        query.select(Arel::Nodes::NamedFunction.new("STRING_AGG", args))
      end

      def string_agg_expression(query, expression, delimiter)
        args = [Arel.sql(expression)]
        args << Arel::Nodes.build_quoted(delimiter) if delimiter
        query.select(Arel::Nodes::NamedFunction.new("STRING_AGG", args))
      end

      def cast_to_string(expr)
        "CAST(#{expr} AS TEXT)"
      end

      def normalize_result(row)
        # Convert booleans to 1/0 for compatibility with test expectations
        row.transform_values do |v|
          if v == true
            1
          else
            v == false ? 0 : v
          end
        end
      end
    end

    class Mysql < Base
      def string_agg(query, column, delimiter)
        separator = delimiter ? " SEPARATOR #{query.connection.quote(delimiter)}" : ""
        table_name = column.relation.table_alias || column.relation.name
        quoted_column = "#{query.connection.quote_table_name(table_name)}.#{query.connection.quote_column_name(column.name)}"
        node = Arel.sql("GROUP_CONCAT(#{quoted_column}#{separator})")
        query.select(node)
      end

      def string_agg_expression(query, expression, delimiter)
        # If the expression contains a comma, wrap with CONCAT_WS to avoid MySQL errors
        expr = expression
        if expr.include?(",")
          # Try to split by comma and wrap with CONCAT_WS(' | ', ...)
          parts = expr.split(",").map(&:strip)
          expr = "CONCAT_WS(' | ', #{parts.join(", ")})"
        end
        separator = delimiter ? " SEPARATOR #{query.connection.quote(delimiter)}" : ""
        node = Arel.sql("GROUP_CONCAT(#{expr}#{separator})")
        query.select(node)
      end

      def cast_to_string(expr)
        "CAST(#{expr} AS CHAR)"
      end
    end
  end

  module AggSQL
    SQL_BUILDERS = {
      count: ->(q, _t, _d) { q.select(Arel.star.count) },
      count_expression: ->(q, _t, d) { q.select(Arel.sql("COUNT(#{d.expression})")) },
      count_distinct: ->(q, t, d) { q.select(t[d.column].count(true)) },
      count_distinct_expression: ->(q, _t, d) { q.select(Arel.sql("COUNT(DISTINCT #{d.expression})")) },
      sum: ->(q, t, d) { q.select(Arel::Nodes::NamedFunction.new("COALESCE", [t[d.column].sum, Arel::Nodes.build_quoted(0)])) },
      sum_expression: ->(q, _t, d) { q.select(Arel.sql("COALESCE(SUM(#{d.expression}), 0)")) },
      avg: ->(q, t, d) { q.select(Arel::Nodes::NamedFunction.new("COALESCE", [t[d.column].average, Arel::Nodes.build_quoted(0.0)])) },
      avg_expression: ->(q, _t, d) { q.select(Arel.sql("COALESCE(AVG(#{d.expression}), 0.0)")) },
      min: ->(q, t, d) { q.select(Arel::Nodes::NamedFunction.new("COALESCE", [t[d.column].minimum, Arel::Nodes.build_quoted(0)])) },
      min_expression: ->(q, _t, d) { q.select(Arel.sql("COALESCE(MIN(#{d.expression}), 0)")) },
      max: ->(q, t, d) { q.select(Arel::Nodes::NamedFunction.new("COALESCE", [t[d.column].maximum, Arel::Nodes.build_quoted(0)])) },
      max_expression: ->(q, _t, d) { q.select(Arel.sql("COALESCE(MAX(#{d.expression}), 0)")) },
      string_agg: lambda { |q, t, d|
        driver = Drivers.for(q.model.connection)
        driver.string_agg(q, t[d.column], d.options&.dig(:delimiter))
      },
      string_agg_expression: lambda { |q, _t, d|
        driver = Drivers.for(q.model.connection)
        expr = driver.cast_to_string(d.expression)
        driver.string_agg_expression(q, expr, d.options&.dig(:delimiter))
      }
    }.freeze

    def self.arel_builder(relation, defn)
      q = relation.except(:select)
      t = q.model.arel_table
      builder = SQL_BUILDERS.fetch(defn.type) { raise "Unknown aggregate type: #{defn.type}" }
      builder.call(q, t, defn).arel
    end

    def self.sql(relation, defn)
      q = relation.except(:select)
      t = q.model.arel_table
      builder = SQL_BUILDERS.fetch(defn.type) { raise "Unknown aggregate type: #{defn.type}" }
      builder.call(q, t, defn).to_sql
    end
  end

  class Query
    def initialize(model, aggs)
      @model = model
      @aggs = aggs
    end

    def build(scope, **kwargs)
      outer = @model.arel_table.alias("batchagg_outer_#{@model.table_name}")
      corr = AssocMagic.new(@model, outer)
      colproj = ColumnProj.new(outer, @model)
      arel = Arel::SelectManager.new.from(outer)
      arel.project(outer[@model.primary_key].as(@model.primary_key.to_s))

      @aggs.reject(&:computed?).each do |agg|
        proj = if agg.custom?
                 Arel.sql(agg.expression.to_s).as(agg.name.to_s)
               elsif agg.column_based?
                 colproj.build(agg, corr, **kwargs)
               else
                 relation = BatchAgg.call_with_optional_kwargs(agg.block, corr, **kwargs)
                 Arel.sql("(#{AggSQL.sql(relation, agg)})").as(agg.name.to_s)
               end
        arel.project(proj)
      end

      arel.where(outer[@model.primary_key].in(Arel.sql("(#{scope.select(@model.primary_key).to_sql})")))
      arel
    end
  end

  class Runner
    def initialize(aggs, model)
      @aggs = aggs
      @model = model
      @query = Query.new(model, aggs)
      @result_class = build_result_class(aggs)
      @driver = Drivers.for(model.connection)
    end

    def only(record, **)
      from(@model.where(@model.primary_key => record.id), **)
    end

    def from(scope, **kwargs)
      rows = scope.klass.connection.select_all(@query.build(scope, **kwargs).to_sql).to_a
      rows.each_with_object({}) do |row, h|
        row = @driver.normalize_result(row)
        id = row[@model.primary_key.to_s]
        h[@model.primary_key ? id : row.keys.first] = @result_class.new(row, **kwargs)
      end
    end

    def combined(scope, **kwargs)
      if @driver.supports_cte?
        cte_name = "batchagg_cte_#{@model.table_name}"
        cte_table = Arel::Table.new(cte_name)
        # Ensure we select all columns needed for joins
        cte_relation = scope.select(@model.column_names)
        cte = Arel::Nodes::As.new(
          cte_table,
          Arel.sql("(#{cte_relation.to_sql})")
        )

        magic_scope = CteAssocMagic.new(@model, cte_table)
        projections = @aggs.reject(&:computed?).map do |agg|
          relation = BatchAgg.call_with_optional_kwargs(agg.block, magic_scope, **kwargs)
          sql_string = AggSQL.sql(relation, agg)
          Arel.sql("(#{sql_string})").as(agg.name.to_s)
        end

        arel = Arel::SelectManager.new.project(*projections).with(cte)
      else
        magic_scope = CombinedAssocMagic.new(scope, join_builder_scope: scope)
        projections = @aggs.reject(&:computed?).map do |agg|
          relation = BatchAgg.call_with_optional_kwargs(agg.block, magic_scope, **kwargs)
          sql_string = AggSQL.sql(relation, agg)
          Arel.sql("(#{sql_string})").as(agg.name.to_s)
        end

        arel = Arel::SelectManager.new.project(*projections)
      end
      result_row = scope.klass.connection.select_one(arel.to_sql).to_h

      result_row = @driver.normalize_result(result_row)
      result_obj = @result_class.new(result_row, **kwargs)

      # Patch: allow computed fields to access the full result via `results:`
      @aggs.select(&:computed?).each do |agg|
        define_singleton = result_obj.method(:define_singleton_method)
        block = agg.block
        has_results_kwarg = block.parameters.any? { |type, name| %i[key keyreq].include?(type) && name == :results }
        next unless has_results_kwarg

        define_singleton.call(agg.name) do |*_args, **_kws|
          @cache[agg.name] ||= block.call(self, **@kwargs, results: result_obj)
        end
      end

      result_obj
    end

    private

    def build_result_class(aggs)
      Class.new do
        define_method(:initialize) do |row, **kwargs|
          @data = row.transform_keys(&:to_sym)
          @cache = {}
          @kwargs = kwargs
        end

        aggs.each do |agg|
          if agg.computed?
            define_method(agg.name) do
              @cache[agg.name] ||= begin
                # Pass kwargs if block accepts them
                block = agg.block
                has_kwargs = block.parameters.any? { |type, _| %i[key keyreq keyrest].include?(type) }
                has_kwargs ? block.call(self, **@kwargs) : block.call(self)
              end
            end
          else
            define_method(agg.name) { @data[agg.name] }
          end
        end
      end
    end
  end

  class Collector
    attr_reader :aggs

    def initialize = @aggs = []

    # Simple aggregates: count(name, &block)
    %i[count computed].each do |type|
      define_method(type) { |name, &block| add(type, name, block) }
    end

    # Column-based aggregates: sum(name, column, &block)
    %i[sum avg min max count_distinct].each do |type|
      define_method(type) { |name, column, &block| add(type, name, block, column: column) }
    end

    # Expression-based aggregates: sum_expression(name, expr, &block)
    %i[sum_expression avg_expression min_expression max_expression count_expression count_distinct_expression].each do |type|
      define_method(type) { |name, expr, &block| add(type, name, block, expression: expr) }
    end

    def column(name, &block) = add(:column, name, block, column: name)
    def custom(name, sql) = add(:custom, name, nil, expression: sql)

    def string_agg(name, column, delimiter: nil, &block)
      add(:string_agg, name, block, column: column, options: { delimiter: delimiter })
    end

    def string_agg_expression(name, expr, delimiter: nil, &block)
      add(:string_agg_expression, name, block, expression: expr, options: { delimiter: delimiter })
    end

    private

    def add(type, name, block, **)
      @aggs << AggregateDef.new(name: name, type: type, block: block, **)
    end
  end

  class Builder
    def initialize(model) = @model = model

    def build_class(&)
      col = Collector.new
      col.instance_eval(&)
      Runner.new(col.aggs, @model)
    end
  end

  module DSL
    def aggregate(model, &)
      Builder.new(model).build_class(&)
    end
  end
end
