require 'ostruct'

module BatchAgg
  class AggregateBuilder
    def initialize(base_model)
      @base_model = base_model
      @aggregates = {}
    end

    def count(name, &block)
      @aggregates[name] = { type: :count, block: block }
    end

    def build_class
      aggregates = @aggregates
      base_model = @base_model

      Class.new do
        attr_reader :record

        def initialize(record)
          @record = record
          @results = self.class.execute_query(record)
        end

        aggregates.each do |name, config|
          define_method(name) do
            @results[name.to_s]
          end
        end

        define_singleton_method :execute_query do |record|
          # Build the main query using Arel
          main_table = base_model.arel_table

          # Create subquery projections for each aggregate
          projections = aggregates.map do |name, config|
            relation = config[:block].call(record)

            # Get the Arel AST for the subquery
            subquery_arel = relation.except(:select).select('COUNT(*)').arel

            # Create a named projection
            subquery_arel.as(name.to_s)
          end

          # Build the main query
          query = main_table
            .project(*projections)
            .where(main_table[:id].eq(record.id))

          # Execute the query
          puts query.to_sql
          result = ActiveRecord::Base.connection.select_all(query.to_sql)
          puts result.inspect

          return OpenStruct.new(result.first)
        end
      end
    end
  end
end

def aggregate(&block)
  # Infer the base model from the block context
  # For now, we'll assume User as the base model
  builder = BatchAgg::AggregateBuilder.new(User)
  builder.instance_eval(&block)
  builder.build_class
end
