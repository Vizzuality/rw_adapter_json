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
  # after_save :clear_cache

  has_many :data_values, dependent: :destroy

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

  def update_data_columns
    self.update(data_columns: Hash[ActiveRecord::Base.connection.execute(update_meta_data).
                                                      map { |v| [v['key'], { type: v['type'] }] }])
    ActiveRecord::Base.clear_reloadable_connections!
  end

  private

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

    def clear_cache
      Rails.cache.delete_matched("*results_#{self.id}*") if Rails.cache.read("*results_#{self.id}*").present?
    end
end
