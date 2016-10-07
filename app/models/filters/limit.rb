# frozen_string_literal: true
module Filters
  class Limit
    def self.apply_limit(limit_params)
      to_limit   = limit_params.is_a?(Array) ? limit_params.join(',').split(',') : limit_params.split(',')
      limit_attr = to_limit[0]
      " Limit #{limit_attr}"
    end
  end
end
