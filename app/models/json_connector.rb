require 'oj'

class JsonConnector
  include ActiveModel::Serialization
  attr_reader :id, :name, :provider, :format, :data_path, :attributes_path

  def initialize(params)
    @dataset_params = params[:dataset] || params[:connector]
    initialize_options
  end

  def data(options = {})
    get_data = JsonService.new(@id, options)
    get_data.connect_data
  end

  def data_columns
    Dataset.find(@id).try(:data_columns)
  end

  def data_horizon
    Dataset.find(@id).try(:data_horizon)
  end

  def new(params)
    @dataset_url = params['dataset_url'] if params['dataset_url'].present?
    @data_path   = params['data_path']   if params['data_path'].present?
    if params['dataset_url'].present?
      @recive_attributes = ConnectorService.connect_to_provider(@dataset_url, @data_path)
      @data = { data: @recive_attributes }
      params = params['data'].present? ? params : params.merge!(@data)
    end
    params = params['data_columns'].present? ? params                      : {}
    params = params['dataset_url'].present?  ? params.except(:dataset_url) : params
    params = params['data_path'].present?    ? params.except(:data_path)   : params
    Dataset.new(params[:dataset].permit!)
  end

  private

    def initialize_options
      @options = DatasetParams.sanitize(@dataset_params)
      @options.keys.each { |k| instance_variable_set("@#{k}", @options[k]) }
    end
end
