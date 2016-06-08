module Filters
  class Select
    def self.apply_select(dataset_id, select_params, aggr_func, aggr_by)
      to_select = if select_params.present?
                    select_params.join(',').split(',')
                  else
                    attribute_keys = Dataset.execute_data_query("SELECT DISTINCT jsonb_object_keys(jsonb_array_elements(data)) as attribute_key FROM datasets WHERE id='#{dataset_id}'")
                    attribute_keys.to_ary.map { |v| v['attribute_key'] }.join(',').split(',')
                  end

      filter = 'WITH t AS (select'

      to_select.each_index do |i|
        filter += ',' if i > 0
        filter += " jsonb_array_elements(data) ->> '#{to_select[i]}' as #{to_select[i]}"
      end

      filter += " from datasets where id='#{dataset_id}') SELECT"

      if aggr_by.present? && aggr_func.present?
        to_aggr = aggr_by.join(',').split(',')

        to_aggr.each_index do |i|
          filter += ',' if i > 0
          filter += " #{aggr_func}(#{to_aggr[i]}::integer) as #{to_aggr[i]}"
        end
      end

      to_select = to_select.delete_if { |p| p.in? to_aggr } if aggr_by.present? && aggr_func.present?

      to_select.each_index do |i|
        filter += ',' if i > 0 || to_aggr.present?
        filter += " #{to_select[i]}"
      end

      filter += ' FROM t'

      filter
    end
  end
end
