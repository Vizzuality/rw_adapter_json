# frozen_string_literal: true
module Filters
  class FilterWhere
    def self.apply_where(filter_params)
      filter_params = filter_params.split.map { |v| v.to_i != 0 ? "'#{v}'" : v }.join(' ')
      " WHERE #{filter_params}"
    end
  end
end
