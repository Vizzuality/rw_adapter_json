require 'rails_helper'

RSpec.describe DatasetServiceJob, type: :job do
  include ActiveJob::TestHelper
  subject(:job) { described_class.perform_later(12345, 'saved') }

  it 'queues the job' do
    expect { job }.to change(ActiveJob::Base.queue_adapter.enqueued_jobs, :size).by(1)
  end

  it 'executes perform' do
    expect(ConnectorService).to receive(:connect_to_dataset_service).with(12345, 'saved')
    perform_enqueued_jobs { job }
  end

  after do
    clear_enqueued_jobs
    clear_performed_jobs
  end
end
