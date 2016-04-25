# == Schema Information
#
# Table name: datasets
#
#  id           :uuid             not null, primary key
#  data_columns :jsonb            default("{}")
#  data         :jsonb            default("[]")
#  data_horizon :integer          default("0")
#  created_at   :datetime         not null
#  updated_at   :datetime         not null
#

class Dataset < ApplicationRecord
  include NullAttributesRemover

  class << self
    def execute_data_query(sql_to_run)
      sql = sanitize_sql(sql_to_run)
      connection.select_all(sql)
    end

    def notifier(object_id, status=nil)
      # DatasetServiceJob.perform_later(object_id, status)
      ConnectorService.connect_to_dataset_service(object_id, status)
    end

    def build_dataset(params)
      params.permit!
      Dataset.new(params)
    end
  end
end
