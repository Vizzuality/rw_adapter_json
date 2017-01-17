# frozen_string_literal: true
module Filters
  class GroupBy
    def self.apply_group_by(group_by_params, count)
      group_by = group_by_params.is_a?(Array) ? group_by_params.join(',') : group_by_params
      if count.present?
        " GROUP BY data->>'#{group_by}'"
      else
        " GROUP BY #{group_by}"
      end
    end
  end
end
