# frozen_string_literal: true
# == Schema Information
#
# Table name: datasets
#
#  id           :uuid             not null, primary key
#  data_columns :jsonb
#  data         :jsonb
#  data_horizon :integer          default(0)
#  created_at   :datetime         not null
#  updated_at   :datetime         not null
#

class Dataset < ApplicationRecord
  after_save :update_data_columns, if: 'data[0].present? && data_columns == data.first'

  class << self
    def execute_data_query(sql_to_run)
      sql = sanitize_sql(sql_to_run)
      connection.select_all(sql)
    end

    def notifier(object_id, status=nil)
      DatasetServiceJob.perform_later(object_id, status)
    end

    def build_dataset(params)
      params.permit!
      Dataset.new(params)
    end
  end

  private

    def update_data_columns
      self.update_attributes(data_columns: ActiveRecord::Base.connection.execute(update_meta_data).map { |v| { v['key'] => { type: v['type'] } } })
    end

    def update_meta_data
      dataset_id = ActiveRecord::Base.send(:sanitize_sql_array, ['id = :dataset_id', dataset_id: self.id])
      <<-SQL
        with types as (
          SELECT
              json_data.key AS key,
              CASE WHEN left(json_data.value::text,1) = '"'  THEN 'string'
                   WHEN json_data.value::text ~ '^-?\d' THEN
                      CASE WHEN json_data.value::text ~ '\.' THEN 'number'
                           ELSE 'integer'
                      END
                   WHEN left(json_data.value::text,1) = '['  THEN 'array'
                   WHEN left(json_data.value::text,1) = '{'  THEN 'object'
                   WHEN json_data.value::text in ('true', 'false')  THEN 'boolean'
                   WHEN json_data.value::text = 'null'  THEN 'null'
                   ELSE 'integer'
              END as type
          FROM datasets, jsonb_each(datasets.data_columns) AS json_data where #{dataset_id}
        )
        select * from types;
      SQL
    end
end
