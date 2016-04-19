module Filters
  class Select
    def self.apply_select(dataset_id, select_params)
      to_select = if select_params.present?
                    select_params.join(',').split(',')
                  else
                    attribute_keys = Dataset.connection.select_all("SELECT DISTINCT jsonb_object_keys(jsonb_array_elements(data)) as attribute_key FROM datasets WHERE id=#{dataset_id}")
                    attribute_keys.to_ary.map { |v| v['attribute_key'] }.join(',').split(',')
                  end

      filter = 'WITH t AS (select'

      to_select.each_index do |i|
        filter += ',' if i > 0
        filter += " jsonb_array_elements(data) ->> '#{to_select[i]}' as #{to_select[i]}"
      end

      filter += " from datasets where id=#{dataset_id}) SELECT"

      to_select.each_index do |i|
        filter += ',' if i > 0
        filter += " #{to_select[i]}"
      end

      filter += ' FROM t'

      filter
    end
  end
end
