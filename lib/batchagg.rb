require "ostruct"

module BatchAgg
  class AggregateDefinition
    attr_reader :name, :type, :block

    def initialize(name, type, block)
      @name = name
      @type = type
      @block = block
    end
  end

  class QueryBuilder
    def initialize(base_model)
      @base_model = base_model
    end

    def build_query_for_record(record, aggregates)
      main_table = @base_model.arel_table
      projections = build_projections(record, aggregates)

      main_table
        .project(*projections)
        .where(main_table[:id].eq(record.id))
    end

    private

    def build_projections(record, aggregates)
      aggregates.map do |aggregate|
        build_projection_for_aggregate(record, aggregate)
      end
    end

    def build_projection_for_aggregate(record, aggregate)
      relation = aggregate.block.call(record)
      subquery = build_count_subquery(relation)
      Arel.sql("(#{subquery})").as(aggregate.name.to_s)
    end

    def build_count_subquery(relation)
      relation.except(:select).select("COUNT(*)").to_sql
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
      result = ActiveRecord::Base.connection.select_all(query.to_sql)
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

  class AggregateResultClass
    def initialize(aggregates, base_model)
      @aggregates = aggregates
      @base_model = base_model
    end

    def only(record)
      SingleRecordResult.new(record, @aggregates, @base_model)
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
  builder = BatchAgg::AggregateBuilder.new(User)
  builder.instance_eval(&block)
  builder.build_class
end
