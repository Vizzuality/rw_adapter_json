# frozen_string_literal: true
module Filters
  class FilterWhere
    def self.apply_where(filter_params)
      " WHERE #{filter_params}"
    end
  end
end
