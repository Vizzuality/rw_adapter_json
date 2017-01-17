# frozen_string_literal: true
module Filters
  class GroupBy
    def self.apply_group_by(group_by_params, count)
      filter = " GROUP BY"
      if count.present?
        group_by = group_by_params.is_a?(Array) ? group_by_params : group_by_params.split(',')
        group_by.each_with_index do |group, i|
          filter += ',' if i.positive?
          filter += " data->>'#{group}'"
        end
      else
        group_by = group_by_params.is_a?(Array) ? group_by_params.join(',') : group_by_params
        filter += " #{group_by}"
      end
      filter
    end
  end
end
