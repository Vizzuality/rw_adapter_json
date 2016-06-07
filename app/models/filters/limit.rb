module Filters
  class Limit
    def self.apply_limit(limit_params)
      to_limit = limit_params.is_a?(Array) ? limit_params.join(',').split(',') : limit_params.split(',')
      filter = ' Limit'

      limit_attr = "#{to_limit[0]}"
      filter += " #{limit_attr}"
      filter
    end
  end
end
