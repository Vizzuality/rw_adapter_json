# frozen_string_literal: true
module Filters
  class FilterWhere
    def self.apply_where(filter_params, count)
      if count.present?
        filter  = 'AND '
        filter += filter_params
        filter
        filter = filter.split(' ').each_slice(4).to_a
        where  = " "
        filter.each_with_index do |params_array, i|
          filter_params_concat   = params_array[0]
          filter_params_key      = params_array[1]
          filter_params_operator = params_array[2]
          filter_params_value    = params_array[3]
          where += " #{filter_params_concat} data->>'#{filter_params_key}' #{filter_params_operator} #{filter_params_value}"
        end
        where
      else
        " WHERE #{filter_params}"
      end
    end
  end
end
