# frozen_string_literal: true
# == Schema Information
#
# Table name: data_values
#
#  id         :uuid             not null, primary key
#  dataset_id :uuid
#  data       :jsonb
#  created_at :datetime         not null
#  updated_at :datetime         not null
#

class DataValue < ApplicationRecord
  belongs_to :dataset

  class << self
    def execute_data_query(sql_to_run)
      sql = sanitize_sql(sql_to_run)
      connection.select_all(sql)
    end
  end
end
