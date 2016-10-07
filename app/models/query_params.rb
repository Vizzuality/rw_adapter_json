# frozen_string_literal: true
class QueryParams < Hash
  def initialize(params)
    sanitized_params = {
      aggr_by:      params['outStatistics'].present? ? build_aggr(params['outStatistics'], ':onStatisticField') : [],
      aggr_func:    params['outStatistics'].present? ? build_aggr(params['outStatistics'], ':statisticType') : [],
      aggr_as:      params['outStatistics'].present? ? build_aggr(params['outStatistics'], ':outStatisticFieldName') : [],
      sql:          params['sql']                        || nil,
      select:       params['outFields']                  || nil,
      order:        params['orderByFields']              || nil,
      filter_where: params['where']                      || nil,
      group:        params['groupByFieldsForStatistics'] || nil,
      limit:        params['limit']                      ||= standard_limit(params)
    }

    super(sanitized_params)
    merge!(sanitized_params)
  end

  def self.sanitize(params)
    new(params)
  end

  private

    def build_aggr(out_statistics, field)
      out_statistics = eval(out_statistics)
      array = []
      out_statistics.each_index do |i|
        array << "#{out_statistics[i][eval(field)]}"
      end
      array
    end

    def standard_limit(params)
      if params.present?
        ['all']
      else
        [1]
      end
    end
end
