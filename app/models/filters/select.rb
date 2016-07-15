module Filters
  class Select
    class << self
      def apply_select(dataset_id, select_params, aggr_func, aggr_by, aggr_as, group_by)
        to_select = to_select(dataset_id, select_params, aggr_by, group_by)

        filter = 'WITH t AS (select'

        to_select.each_index do |i|
          filter += ',' if i > 0
          filter += " jsonb_array_elements(data) ->> '#{to_select[i]}' as #{to_select[i]}"
        end

        filter += " from datasets where id='#{dataset_id}') SELECT"

        if aggr_by.present? && aggr_func.present?
          to_aggr   = aggr_by.join(',').split(',')
          as_aggr   = aggr_as.join(',').split(',')
          func_aggr = aggr_func.join(',').split(',')

          to_aggr.each_index do |i|
            as_aggr[i] = func_aggr[i] if as_aggr[i].blank?
            filter += ',' if i > 0
            filter += " #{func_aggr[i]}(#{to_aggr[i]}::float) as #{as_aggr[i]}"
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

      def to_select(dataset_id, select_params, aggr_by, group_by)
        if select_params.present? || aggr_by.present?
          return select_params.include?('*') ? self_attributes(dataset_id) : select_params.split(',') unless aggr_by.present?

          select_params = if select_params.blank? || select_params.include?('*')
                            []
                          else
                            select_params.split(',')
                          end

          select_params << aggr_by.split(',')
          select_params << group_by.split(',')
          select_params.flatten.uniq
        else
          self_attributes(dataset_id)
        end
      end

      def self_attributes(dataset_id)
        attribute_keys = Dataset.execute_data_query("SELECT DISTINCT jsonb_object_keys(jsonb_array_elements(data)) as attribute_key FROM datasets WHERE id='#{dataset_id}'")
        attribute_keys.to_ary.map { |v| v['attribute_key'] }.join(',').split(',')
      end
    end
  end
end
