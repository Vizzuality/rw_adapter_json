# frozen_string_literal: true
require 'oj'

class JsonConnector
  extend ActiveModel::Naming
  include ActiveModel::Serialization
  attr_reader :id, :table_name

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

  class << self
    def build_params(options, method)
      dataset_url = options['connector_url'] if options['connector_url'].present?
      data_path   = options['data_path']     if options['data_path'].present?
      params = {}
      data = if options['connector_url'].present? && options['data'].blank?
               ConnectorService.connect_to_provider(dataset_url, data_path)
             else
               Oj.load(options['data']) if options['data']
             end

      params['data'] = data.each_index do |i|
                         data[i].merge!(data_id: SecureRandom.uuid) if data[i]['data_id'].blank?
                       end rescue []

      params['id'] = options['id']
      params['data_columns'] = if params['data'].present? && options['data_columns'].blank?
                                 params['data'][0]
                               else
                                 options['data_columns'].present? ? Oj.load(options['data_columns']) : nil
                               end if method == 'build_dataset'

      params
    end

    def sleeping_connection
      ActiveRecord::Base.clear_reloadable_connections!
      sleep 15
    end

    def concatenate_data(dataset_id, params)
      full_data  = params['data']

      full_data.reject(&:nil?).in_groups_of(5000).each do |group|
        group = group.reject(&:nil?)
        ActiveRecord::Base.connection.execute("UPDATE datasets SET data=data || '#{group.to_json}' WHERE id = '#{dataset_id}'")
        sleeping_connection unless Rails.env.test?
      end
    end

    def build_dataset(options)
      params = build_params(options, 'build_dataset')
      params_for_create = params.except('data').merge(data: [])
      dataset = Dataset.new(params_for_create)

      if dataset.save
        concatenate_data(dataset.id, params)
      end
      dataset
    end

    def update_dataset(options)
      dataset           = Dataset.find(options['id'])
      params            = build_params(options, 'update_dataset')
      params_for_update = params.except('data')

      if dataset.update(params_for_update)
        concatenate_data(dataset.id, params) if params['data'].present?
      end
      dataset
    end

    def overwrite_data(options)
      dataset           = Dataset.find(options['id'])
      params            = build_params(options, 'build_dataset')
      params_for_update = params.except('data').merge(data: [])

      if dataset.update(params_for_update)
        concatenate_data(dataset.id, params)
        dataset.update_data_columns if dataset.data_columns.present?
      end
      dataset
    end

    def update_data_object(options)
      dataset      = Dataset.find(options['id'])
      dataset_data = dataset.data
      data         = dataset_data.find_all { |d| d['data_id'] == options['data_id'] }

      data = data.each_index do |i|
               data[i].merge!(Oj.load(options['data']))
             end

      new_data = dataset_data.delete_if { |d| d['data_id'] == options['data_id'] }
      new_data = new_data.inject(data, :<<)
      dataset.update(data: new_data)
      dataset
    end

    def delete_data_object(options)
      dataset  = Dataset.find(options['id'])
      new_data = dataset.data.delete_if { |d| d['data_id'] == options['data_id'] }
      dataset.update(data: new_data)
      dataset
    end
  end

  private

    def initialize_options
      @options = DatasetParams.sanitize(@dataset_params)
      @options.keys.each { |k| instance_variable_set("@#{k}", @options[k]) }
    end
end
