# frozen_string_literal: true
module Filters
  class FilterWhere
    # Refactoring for large where. Make it more flexible!
    def self.apply_where(filter_params, count)
      if count.present?
        filter_params_key      = filter_params.split(" ")[0]
        filter_params_operator = filter_params.split(" ")[1]
        filter_params_value    = filter_params.split(" ")[2]
        " AND data->>'#{filter_params_key}' #{filter_params_operator} #{filter_params_value}"
      else
        " WHERE #{filter_params}"
      end
    end
  end
end
