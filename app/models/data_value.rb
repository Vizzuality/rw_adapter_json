# frozen_string_literal: true
class DataValue < ApplicationRecord
  belongs_to :dataset

  class << self
    def execute_data_query(sql_to_run)
      sql = sanitize_sql(sql_to_run)
      connection.select_all(sql)
    end
  end
end
