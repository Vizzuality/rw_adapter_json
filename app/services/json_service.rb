# frozen_string_literal: true
require 'oj'

class JsonService
  def initialize(id, options = {})
    @id = id
    @options_hash = options
    initialize_options
    initialize_query_options if @sql.present?
  end

  def connect_data
    if @options_hash.present?
      options_query
    else
      index_query
    end
  end

  private

    def initialize_options
      @options = QueryParams.sanitize(@options_hash)
      @options.keys.each { |k| instance_variable_set("@#{k}", @options[k]) }
    end

    def initialize_query_options
      @options_query = QueryParams.sanitize(recive_valid_query)
      @options_query.keys.each { |k| instance_variable_set("@#{k}", @options_query[k]) }
    end

    def recive_valid_query
      qs_to_hash(QueryService.connect_to_query_service(@sql)).merge!(limit: @limit)
    end

    def index_query
      # Dataset.find(@id).data
      Dataset.select(:id).where(id: options['id']).first.data_values.first
    end

    def options_query
      # SELECT .. FROM data
      filter = Filters::Select.apply_select(@id, @select, @aggr_func, @aggr_by, @aggr_as, @group, @count)
      # WHERE
      filter += Filters::FilterWhere.apply_where(@filter_where) if @filter_where.present?
      # GROUP BY
      filter += Filters::GroupBy.apply_group_by(@group) if @group.present?
      # ORDER BY
      filter += Filters::Order.apply_order(@order) if @order.present?
      # LIMIT
      filter += Filters::Limit.apply_limit(@limit) if @limit.present? && !@limit.include?('all')
      begin
        Dataset.execute_data_query(filter).to_a
      rescue => e
        error = Oj.dump({ error: [e.cause.to_s.split(' ').join(' ')] })
        Oj.load(error)
      end
    end

    def qs_to_hash(query_string)
      key_val = query_string.gsub('?','').split('&').inject({}) do |result, q|
                  k,v = q.split(/^(.*?)=/) - [""]
                  if v.present?
                    result.merge({ k => v })
                  elsif !result.key?(k)
                    result.merge({ k => true })
                  else
                    result
                  end
                end
      key_val
    end
end
