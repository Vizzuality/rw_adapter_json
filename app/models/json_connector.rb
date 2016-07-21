require 'oj'

class JsonConnector
  extend ActiveModel::Naming
  include ActiveModel::Serialization
  attr_reader :id

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

  def self.build_params(options, method)
    dataset_url = options['connector_url'] if options['connector_url'].present?
    data_path   = options['data_path']     if options['data_path'].present?
    params = {}
    data = if options['connector_url'].present? && options['data'].blank?
             ConnectorService.connect_to_provider(dataset_url, data_path)
           else
             Oj.load(options['data'])
           end

    params['data'] = data.each_index do |i|
                       data[i].merge!(data_id: SecureRandom.uuid) if data[i]['data_id'].blank?
                     end

    params['id'] = options['id']
    params['data_columns'] = if params['data'].present? && options['data_columns'].blank?
                               params['data'].first
                             else
                               options['data_columns'].present? ? Oj.load(options['data_columns']) : nil
                             end if method.include?('build_dataset')

    params
  end

  def self.build_dataset(options)
    params = build_params(options, 'build_dataset')
    Dataset.new(params)
  end

  def self.update_dataset(options)
    params  = build_params(options, 'update_dataset')
    dataset = Dataset.find(options['id'])

    params_for_update = params.except(:data)
    params_for_update = params_for_update.merge!(data: dataset.data.inject(params['data'], :<<) )

    dataset.update(params_for_update)
  end

  def self.update_data_object(options)
    dataset      = Dataset.find(options['id'])
    dataset_data = dataset.data
    data         = dataset_data.find_all { |d| d['data_id'] == options['data_id'] }

    data = data.each_index do |i|
             data[i].merge!(Oj.load(options['data']))
           end

    new_data = dataset_data.delete_if { |d| d['data_id'] == options['data_id'] }
    new_data = new_data.inject(data, :<<)
    dataset.update(data: new_data)
  end

  def self.delete_data_object(options)
    dataset  = Dataset.find(options['id'])
    new_data = dataset.data.delete_if { |d| d['data_id'] == options['data_id'] }
    dataset.update(data: new_data)
  end

  private

    def initialize_options
      @options = DatasetParams.sanitize(@dataset_params)
      @options.keys.each { |k| instance_variable_set("@#{k}", @options[k]) }
    end
end
