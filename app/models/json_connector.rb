class JsonConnector < ApplicationRecord
  self.table_name = :json_connectors
  attr_reader :id, :connector_name, :provider, :format, :connector_path, :data_attributes

  include ReadOnlyModel

  has_one :dataset, as: :dateable, inverse_of: :dateable

  def initialize(params)
    @dataset_params = params[:dataset]
    initialize_options
  end

  def data(options = {})
    get_data = JsonService.new(@id, options)
    get_data.connect_data
  end

  private

    def initialize_options
      @options = DatasetParams.sanitize(@dataset_params)
      @options.keys.each { |k| instance_variable_set("@#{k}", @options[k]) }
    end
end
