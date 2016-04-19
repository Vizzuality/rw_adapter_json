module Filters
  class GroupBy
    def self.apply_group_by(group_by_params)
      group_by = group_by_params.join(',')

      filter = " GROUP BY #{group_by}"
      filter
    end
  end
end
