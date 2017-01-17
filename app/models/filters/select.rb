# frozen_string_literal: true
module Filters
  class Select
    class << self
      def apply_select(dataset_id, select_params, aggr_func, aggr_by, aggr_as, group_by, count)
        if count.present?
          filter = "select count(*) from data_values where dataset_id='#{dataset_id}'"
        else
          to_select = to_select(dataset_id, select_params, aggr_by, group_by)

          filter = 'WITH t AS (select'

          self_attributes(dataset_id).each_with_index do |attr, i|
            filter += ',' if i.positive?
            filter += " data ->> '#{attr}' as #{attr}"
          end

          filter += " from data_values where dataset_id='#{dataset_id}') SELECT"

          if aggr_by.present? && aggr_func.present?
            to_aggr   = aggr_by.join(',').split(',')
            as_aggr   = aggr_as.join(',').split(',')
            func_aggr = aggr_func.join(',').split(',')

            to_aggr.each_with_index do |attr, i|
              as_aggr[i] = attr if as_aggr[i].blank?
              filter += ',' if i.positive?
              filter += " #{func_aggr[i]}(#{to_aggr[i]}::float) as #{as_aggr[i]}"
            end
          end

          to_select = to_select.delete_if { |p| p.in? to_aggr } if aggr_by.present? && aggr_func.present?

          to_select.each_with_index do |attr, i|
            filter += ',' if i.positive? || to_aggr.present?
            filter += " #{attr}"
          end

          filter += ' FROM t'
        end
        filter
      end

      def to_select(dataset_id, select_params=nil, aggr_by=nil, group_by=nil)
        if select_params.present? || aggr_by.present?
          return select_params.include?('*') || select_params.blank? ? self_attributes(dataset_id) : select_params.split(',') unless aggr_by.present?

          select_params = if select_params.blank? || select_params.include?('*')
                            []
                          else
                            select_params.split(',')
                          end

          select_params << aggr_by.split(',')  if aggr_by.present?
          select_params << group_by.split(',') if group_by.present?
          select_params.flatten.uniq
        else
          self_attributes(dataset_id)
        end
      end

      def self_attributes(dataset_id)
        attribute_keys = DataValue.execute_data_query("SELECT DISTINCT jsonb_object_keys(data) as attribute_key FROM data_values WHERE dataset_id='#{dataset_id}'")
        attribute_keys.to_ary.map { |v| v['attribute_key'] }.join(',').split(',')
      end
    end
  end
end
