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

  def self.build_dataset(options)
    dataset_url = options['connector_url'] if options['connector_url'].present?
    data_path   = options['data_path']     if options['data_path'].present?
    params = {}
    params['data'] = if options['connector_url'].present? && options['data'].blank?
                       ConnectorService.connect_to_provider(dataset_url, data_path)
                     else
                       options['data']
                     end
    params['id'] = options['id']
    params['data_columns'] = if options['connector_url'].present? && options['data_columns'].blank?
                               params['data'].first
                             else
                               options['data_columns'] || {}
                             end
    Dataset.new(params)
  end

  private

    def initialize_options
      @options = DatasetParams.sanitize(@dataset_params)
      @options.keys.each { |k| instance_variable_set("@#{k}", @options[k]) }
    end
end
