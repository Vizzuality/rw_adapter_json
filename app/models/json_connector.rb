# frozen_string_literal: true
require 'oj'

class JsonConnector
  extend ActiveModel::Naming
  include ActiveModel::Serialization
  include HashFinder
  attr_reader :id, :table_name

  def initialize(params)
    @dataset_params = if params[:connector].present? && params[:connector].to_unsafe_hash.recursive_has_key?(:attributes)
                        params[:connector][:dataset][:data].merge(params[:connector][:dataset][:data][:attributes].to_unsafe_hash)
                      else
                        params[:dataset] || params[:connector]
                      end
    initialize_options
  end

  def data(options = {})
    cache_options  = "results_#{self.id}"
    cache_options += "_#{options}" if options.present?

    if results = Rails.cache.read(cache_key(cache_options))
      results
    else
      get_data = JsonService.new(@id, options)
      results  = get_data.connect_data

      Rails.cache.write(cache_key(cache_options), results.to_a) if results.present?
    end
    results
  end

  def cache_key(cache_options)
    "query_#{ cache_options }"
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
      if method == 'build_dataset'
        params['data_columns'] = if params['data'].present? && options['data_columns'].blank?
                                   params['data'][0]
                                 else
                                   options['data_columns'].present? ? Oj.load(options['data_columns']) : nil
                                 end
      end

      params
    end

    def sleep_connection
      ActiveRecord::Base.clear_reloadable_connections!
      sleep 1
    end

    def concatenate_data(dataset_id, params, date=nil)
      full_data = params['data']
      full_data = full_data.reject(&:nil?).freeze
      full_data

      full_data.in_groups_of(1000).each do |group|
        group = group.reject(&:nil?)
        group = group.map! { |data| data.each { |key,value| data[key] = value.to_datetime.iso8601 if key.in?(date) } } if date.present?
        group = group.map! { |data| data.each { |key,value| data[key] = value.gsub("'", "´") if value.is_a?(String) } }
        group
        query = ActiveRecord::Base.send(:sanitize_sql_array, ["UPDATE datasets SET data=data || '#{group.to_json}' WHERE id = ?", dataset_id])
        ActiveRecord::Base.connection.execute(query)
        sleep_connection unless Rails.env.test?
      end
    end

    def build_dataset(options)
      params            = build_params(options, 'build_dataset')
      params_for_create = params.except('data').merge(data: [])
      dataset           = Dataset.new(params_for_create)
      date              = options['legend']['date'] if options['legend'].present? && options['legend']['date'].present?

      if dataset.save
        concatenate_data(dataset.id, params, date)
        dataset = Dataset.select(:id, :data_columns).where(id: dataset.id).first
        if dataset.data_columns.present? && options['data_columns'].blank?
          dataset.update_data_columns
        end
      end
      dataset
    end

    def update_dataset(options)
      dataset           = Dataset.find(options['id'])
      params            = build_params(options, 'update_dataset')
      params_for_update = params.except('data')
      date              = options['legend']['date'] if options['legend'].present? && options['legend']['date'].present?

      if dataset.update(params_for_update)
        concatenate_data(dataset.id, params, date) if params['data'].present?
      end
      dataset
    end

    def overwrite_data(options)
      dataset           = Dataset.select(:id, :data_columns).where(id: options['id']).first
      params            = build_params(options, 'build_dataset')
      params_for_update = params.except('data').merge(data: [])
      date              = options['legend']['date'] if options['legend'].present? && options['legend']['date'].present?

      if dataset.update(params_for_update)
        concatenate_data(dataset.id, params, date)
        if dataset.data_columns.present? && options['data_columns'].blank?
          dataset.update_data_columns
        end
      end
      dataset
    end

    def update_data_object(options)
      dataset        = Dataset.find(options['id'])
      dataset_id     = dataset.id
      dataset_data   = dataset.data
      data_to_update = dataset_data.find_all { |d| d['data_id'] == options['data_id'] }
      data_index     = dataset_data.index(data_to_update[0])
      date           = options['legend']['date'] if options['legend'].present? && options['legend']['date'].present?

      delete_specific_data(data_index, dataset_id)

      data = data_to_update.each_index do |i|
               data_to_update[i].merge!(Oj.load(options['data']))
             end
      data = data.map! { |data| data.each { |key,value| data[key] = value.to_datetime.iso8601 if key.in?(date) } } if date.present?
      data = data.map! { |data| data.each { |key,value| data[key] = value.gsub("'", "´") if value.is_a?(String) } }

      query = ActiveRecord::Base.send(:sanitize_sql_array, ["UPDATE datasets SET data=data::jsonb || '#{data.to_json}' WHERE  id = ?", dataset_id])
      ActiveRecord::Base.connection.execute(query)
      dataset
    end

    def delete_data_object(options)
      dataset        = Dataset.find(options['id'])
      dataset_id     = dataset.id
      dataset_data   = dataset.data
      data_to_update = dataset_data.find_all { |d| d['data_id'] == options['data_id'] }
      data_index     = dataset_data.index(data_to_update[0])
      delete_specific_data(data_index, dataset_id)
      dataset
    end

    def delete_specific_data(data_index, dataset_id)
      query = ActiveRecord::Base.send(:sanitize_sql_array, ["UPDATE datasets SET data=data::jsonb - #{data_index} WHERE  id = ?", dataset_id])
      ActiveRecord::Base.connection.execute(query)
    end
  end

  private

    def initialize_options
      @options = DatasetParams.sanitize(@dataset_params)
      @options.keys.each { |k| instance_variable_set("@#{k}", @options[k]) }
    end
end
