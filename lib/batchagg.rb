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
            relation = config[:block].call(record) # This is an ActiveRecord::Relation

            # Get the ActiveRecord::Relation for the subquery count
            # This relation will have its own context for bind parameters (e.g., from record.id)
            subquery_count_relation = relation.except(:select).select('COUNT(*)')

            # Generate SQL for the subquery; ActiveRecord resolves its bind parameters here
            subquery_sql = subquery_count_relation.to_sql

            # Create a named projection using Arel.sql to treat the subquery SQL as a literal
            # and then alias it with .as()
            Arel.sql("(#{subquery_sql})").as(name.to_s)
          end

          # Build the main query
          query = main_table
            .project(*projections)
            .where(main_table[:id].eq(record.id))

          # Execute the query
          result = ActiveRecord::Base.connection.select_all(query.to_sql)

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
