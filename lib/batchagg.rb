# frozen_string_literal: true

require "active_record"

module BatchAgg
  AggregateDef = Struct.new(:name, :type, :block, :column, :expression, :options, keyword_init: true) do
    def column_based? = type == :column
    def computed? = type == :computed
    def custom? = type == :custom
    def block? = !block.nil?
  end

  class AssocMagic
    def initialize(model, outer_table)
      @model = model
      @outer_table = outer_table
    end

    def method_missing(name, *args, &)
      reflection = @model.reflect_on_association(name)
      if reflection
        target = reflection.klass
        if reflection.through_reflection
          # has_many :through
          through = reflection.through_reflection
          source = reflection.source_reflection
          final_target = reflection.klass.arel_table
          through_table = through.klass.arel_table

          cond1 = if source.macro == :belongs_to
                    through_table[source.foreign_key].eq(final_target[source.association_primary_key])
                  else
                    final_target[source.foreign_key].eq(through_table[source.active_record_primary_key])
                  end

          cond2 = if through.macro == :belongs_to
                    @outer_table[through.foreign_key].eq(through_table[through.association_primary_key])
                  else
                    through_table[through.foreign_key].eq(@outer_table[through.active_record_primary_key])
                  end

          subquery = through_table.project(Arel.sql("1")).where(cond1.and(cond2))
          target.where(subquery.exists)
        elsif reflection.macro == :belongs_to
          target.where(target.arel_table[reflection.association_primary_key].eq(@outer_table[reflection.foreign_key]))
        else
          target.where(target.arel_table[reflection.foreign_key].eq(@outer_table[reflection.active_record_primary_key]))
        end
      else
        super
      end
    end

    def respond_to_missing?(name, *)
      @model.reflect_on_association(name) || super
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
        block = defn.block
        has_kwargs = block.parameters.any? { |type, _| %i[key keyreq keyrest].include?(type) }

        val = begin
          if has_kwargs
            block.call(@attr_magic, **)
          else
            block.call(@attr_magic)
          end
        rescue StandardError
          nil
        end
        return val.as(defn.name.to_s) if val.is_a?(Arel::Attributes::Attribute) || val.is_a?(Arel::Nodes::SqlLiteral)

        rel = if has_kwargs
                block.call(corr, **)
              else
                block.call(corr)
              end
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
    def self.sql(relation, defn)
      q = relation.except(:select)
      t = q.model.arel_table
      driver = Drivers.for(q.model.connection)
      case defn.type
      when :count
        q.select(Arel.star.count).to_sql
      when :count_expression
        q.select(Arel.sql("COUNT(#{defn.expression})")).to_sql
      when :count_distinct
        q.select(t[defn.column].count(true)).to_sql
      when :count_distinct_expression
        q.select(Arel.sql("COUNT(DISTINCT #{defn.expression})")).to_sql
      when :sum
        q.select(Arel::Nodes::NamedFunction.new("COALESCE", [t[defn.column].sum, Arel::Nodes.build_quoted(0)])).to_sql
      when :sum_expression
        q.select(Arel.sql("COALESCE(SUM(#{defn.expression}), 0)")).to_sql
      when :avg
        q.select(Arel::Nodes::NamedFunction.new("COALESCE", [t[defn.column].average, Arel::Nodes.build_quoted(0.0)])).to_sql
      when :avg_expression
        q.select(Arel.sql("COALESCE(AVG(#{defn.expression}), 0.0)")).to_sql
      when :min
        q.select(Arel::Nodes::NamedFunction.new("COALESCE", [t[defn.column].minimum, Arel::Nodes.build_quoted(0)])).to_sql
      when :min_expression
        q.select(Arel.sql("COALESCE(MIN(#{defn.expression}), 0)")).to_sql
      when :max
        q.select(Arel::Nodes::NamedFunction.new("COALESCE", [t[defn.column].maximum, Arel::Nodes.build_quoted(0)])).to_sql
      when :max_expression
        q.select(Arel.sql("COALESCE(MAX(#{defn.expression}), 0)")).to_sql
      when :string_agg
        delim = defn.options&.dig(:delimiter)
        driver.string_agg(q, t[defn.column], delim).to_sql
      when :string_agg_expression
        delim = defn.options&.dig(:delimiter)
        # Use driver-specific cast for string expressions
        expr = defn.expression
        expr = driver.cast_to_string(expr)
        driver.string_agg_expression(q, expr, delim).to_sql
      else
        raise "Unknown aggregate type: #{defn.type}"
      end
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
      arel = Arel::SelectManager.new(scope.klass.connection)
      arel.from(outer)
      arel.project(outer[@model.primary_key].as(@model.primary_key.to_s))
      @aggs.reject(&:computed?).each do |agg|
        proj =
          if agg.custom?
            Arel.sql("#{agg.expression}").as(agg.name.to_s)
          elsif agg.column_based?
            colproj.build(agg, corr, **kwargs)
          else
            block = agg.block
            relation = if block.parameters.any? { |type, _| %i[key keyreq keyrest].include?(type) }
                         block.call(corr, **kwargs)
                       else
                         block.call(corr)
                       end
            Arel.sql("(#{AggSQL.sql(relation, agg)})").as(agg.name.to_s)
          end
        arel.project(proj)
      end
      arel.where(
        outer[@model.primary_key].in(
          Arel.sql("(#{scope.select(@model.primary_key).to_sql})")
        )
      )
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

    def from(scope, **)
      rows = scope.klass.connection.select_all(@query.build(scope, **).to_sql).to_a
      rows.each_with_object({}) do |row, h|
        row = @driver.normalize_result(row)
        id = row[@model.primary_key.to_s]
        h[@model.primary_key ? id : row.keys.first] = @result_class.new(row)
      end
    end

    private

    def build_result_class(aggs)
      klass = Class.new do
        define_method(:initialize) do |row|
          @data = row.transform_keys(&:to_sym)
          @cache = {}
        end
      end
      aggs.reject(&:computed?).each do |agg|
        klass.define_method(agg.name) { @data[agg.name] }
      end
      aggs.select(&:computed?).each do |agg|
        klass.define_method(agg.name) do
          @cache[agg.name] ||= instance_exec(self, &agg.block)
        end
      end
      klass
    end
  end

  class Collector
    attr_reader :aggs

    def initialize = @aggs = []
    def column(name, &block) = add(:column, name, block, column: name)
    def count(name, &block) = add(:count, name, block)
    def count_distinct(name, column, &block) = add(:count_distinct, name, block, column: column)
    def count_expression(name, expr, &block) = add(:count_expression, name, block, expression: expr)
    def count_distinct_expression(name, expr, &block) = add(:count_distinct_expression, name, block, expression: expr)
    def sum(name, column, &block) = add(:sum, name, block, column: column)
    def sum_expression(name, expr, &block) = add(:sum_expression, name, block, expression: expr)
    def avg(name, column, &block) = add(:avg, name, block, column: column)
    def avg_expression(name, expr, &block) = add(:avg_expression, name, block, expression: expr)
    def min(name, column, &block) = add(:min, name, block, column: column)
    def min_expression(name, expr, &block) = add(:min_expression, name, block, expression: expr)
    def max(name, column, &block) = add(:max, name, block, column: column)
    def max_expression(name, expr, &block) = add(:max_expression, name, block, expression: expr)
    def string_agg(name, column, delimiter: nil, &block) = add(:string_agg, name, block, column: column, options: { delimiter: delimiter })
    def string_agg_expression(name, expr, delimiter: nil, &block) = add(:string_agg_expression, name, block, expression: expr, options: { delimiter: delimiter })
    def computed(name, &block) = add(:computed, name, block)
    def custom(name, sql) = add(:custom, name, nil, expression: sql)

    private

    def add(type, name, block, column: nil, expression: nil, options: nil)
      @aggs << AggregateDef.new(name: name, type: type, block: block, column: column, expression: expression, options: options)
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
