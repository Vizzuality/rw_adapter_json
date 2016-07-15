class QueryParams < Hash
  def initialize(params)
    sanitized_params = {
      aggr_by:      params['outStatistics'].present? ? build_aggr_by(params['outStatistics']) : [],
      aggr_func:    params['outStatistics'].present? ? build_aggr_func(params['outStatistics']) : [],
      aggr_as:      params['outStatistics'].present? ? build_aggr_as(params['outStatistics']) : [],
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

    def build_aggr_by(out_statistics)
      out_statistics = eval(out_statistics)
      array = []
      out_statistics.each_index do |i|
        array << "#{out_statistics[i][:onStatisticField]}"
      end
      array
    end

    def build_aggr_func(out_statistics)
      out_statistics = eval(out_statistics)
      array = []
      out_statistics.each_index do |i|
        array << "#{out_statistics[i][:statisticType]}"
      end
      array
    end

    def build_aggr_as(out_statistics)
      out_statistics = eval(out_statistics)
      array = []
      out_statistics.each_index do |i|
        array << "#{out_statistics[i][:outStatisticFieldName]}"
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
