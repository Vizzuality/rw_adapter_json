# frozen_string_literal: true
class DatasetServiceJob < ApplicationJob
  queue_as :default

  def perform(dataset_id, status)
    ConnectorService.connect_to_dataset_service(dataset_id, status)
  end
end
