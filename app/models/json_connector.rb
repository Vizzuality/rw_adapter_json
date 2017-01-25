# frozen_string_literal: true
require 'curb'
require 'typhoeus'
require 'uri'
require 'oj'
require 'yajl'

class JsonConnector
  extend ActiveModel::Naming
  include ActiveModel::Serialization
  include HashFinder
  attr_reader :id, :table_name

  FLUSH_EVERY = 500

  def initialize(params)
    @dataset_params = if params[:connector].present? && params[:connector].to_unsafe_hash.recursive_has_key?(:attributes)
                        params[:connector][:dataset][:data].merge(params[:connector][:dataset][:data][:attributes].to_unsafe_hash)
                      else
                        params[:dataset] || params[:connector]
                      end
    initialize_options
  end

  def data(options = {})
    get_data = JsonService.new(@id, options)
    results  = get_data.connect_data
    results
  end

  def cache_key(cache_options)
    "query_#{ cache_options }"
  end

  def data_columns
    dataset = Dataset.select(:id, :data_columns).find(@id)
    dataset.try(:data_columns)
  end

  def data_horizon
    dataset = Dataset.select(:id, :data_horizon).find(@id)
    dataset.try(:data_horizon)
  end

  class << self
    def build_params(options, method)
      dataset_url = options['connector_url'] if options['connector_url'].present?
      data_path   = options['data_path']     if options['data_path'].present?
      params = {}
      new_data = if options['connector_url'].present? && options['data'].blank?
                   ConnectorService.connect_to_provider(dataset_url, data_path, method, options['id'])
                 else
                   Oj.load(options['data']) if options['data']
                 end

      params['data'] = new_data || []

      params['id'] = options['id']
      if method == 'build_dataset'
        params['data_columns'] = if params['data'].present? && options['data_columns'].blank?
                                   params['data'][0]
                                 else
                                   options['data_columns'].present? ? Oj.load(options['data_columns']) : {}
                                 end
      end

      params
    end

    def gc_rebuild
      GC.start(full_mark: false, immediate_sweep: false)
    end

    def concatenate_data(dataset_id, params, date=nil)
      thunk = lambda do |key,value|
        case value
        when String then value.strip!
        when Hash   then value.each(&thunk)
        when Array  then value.each { |vv| vv.strip! }
        end
      end

      group = []
      batch_size = FLUSH_EVERY

      if params['data'].is_a?(Hash) && params['data'].key?(:file_name)
        full_data = YAJI::Parser.new(File.open("#{params['data'][:file_name]}"))
        if params['data'][:path].present?
          path  = params['data'][:path][0].to_s
          path += "/#{params['data'][:path][1].to_s}" if params['data'][:path][1].present?
          path += "/#{params['data'][:path][2].to_s}" if params['data'][:path][2].present?
          path += "/#{params['data'][:path][3].to_s}" if params['data'][:path][3].present?
          path = "/#{path}/"
        else
          path = '/'
        end

        full_data.each("#{path}") do |obj|
          obj  = obj.symbolize_keys!.each(&thunk)
          data = sanitize_data(obj, date)
          group << DataValue.new(id: data['id'], dataset_id: dataset_id, data: data)
          if group.size >= batch_size
            DataValue.import group, validate: false, batch_size: 100
            group = []
            gc_rebuild
          end
        end
        if group.size <= batch_size
          DataValue.import group, validate: false, batch_size: 100
          group = []
          gc_rebuild
        end
        File.delete("tmp/import/#{dataset_id}.json") if File.exist?("#{params['data'][:file_name]}")
      else
        full_data = params['data']
        full_data = full_data.reject(&:nil?).freeze
        full_data

        full_data.each do |obj|
          obj  = obj.symbolize_keys!.each(&thunk)
          data = sanitize_data(obj, date)
          group << DataValue.new(id: data['id'], dataset_id: dataset_id, data: data)
          if group.size >= batch_size
            DataValue.import group, validate: false, batch_size: 100
            group = []
            gc_rebuild
          end
        end
        if group.size <= batch_size
          DataValue.import group, validate: false, batch_size: 100
          group = []
          gc_rebuild
        end
      end
    end

    def sanitize_data(obj, date)
      obj = obj.each { |key,value| obj[key] = value.to_datetime.iso8601 if key.in?(date)                } if date.present?
      obj = obj.each { |key,value| obj[key] = value.gsub("'", "´").gsub("?", "") if value.is_a?(String) }
      obj = obj[:data_id].blank? ? obj.merge!(data_id: SecureRandom.uuid) : obj
      obj
    end

    def concatenate_data_columns(dataset_id)
      dataset = Dataset.select(:id, :data_columns, :data_horizon).find(dataset_id)
      if dataset.data_values.any?
        first_data = dataset.data_values.first.data
        dataset.update(data_columns: first_data)
        gc_rebuild
      end
    end

    def build_dataset(options)
      params            = build_params(options, 'build_dataset')
      params_for_create = params.except('data').merge(data: [])
      dataset           = Dataset.new(params_for_create)
      date              = options['legend']['date'] if options['legend'].present? && options['legend']['date'].present?

      if dataset.save
        concatenate_data(options['id'], params, date)
        if options['data_columns'].blank?
          concatenate_data_columns(options['id'])
          dataset.update_data_columns
        end
      end
      dataset
    end

    def update_dataset(options)
      dataset           = Dataset.select(:id, :data_columns, :data_horizon).find(options['id'])
      params            = build_params(options, 'update_dataset')
      params_for_update = params.except('data')
      date              = options['legend']['date'] if options['legend'].present? && options['legend']['date'].present?

      if dataset.update(params_for_update)
        concatenate_data(options['id'], params, date) if params['data'].present?
      end
      dataset
    end

    def overwrite_data(options)
      dataset           = Dataset.select(:id, :data_columns).find(options['id'])
      params            = build_params(options, 'build_dataset')
      params_for_update = params.except('data')
      date              = options['legend']['date'] if options['legend'].present? && options['legend']['date'].present?

      if dataset.update(params_for_update)
        dataset.data_values.delete_all
        concatenate_data(options['id'], params, date)
        if options['data_columns'].blank?
          concatenate_data_columns(options['id'])
          dataset.update_data_columns
        end
      end
      dataset
    end

    def update_data_object(options)
      dataset    = Dataset.select(:id, :data_columns, :data_horizon).find(options['id'])
      dataset_id = options['id']
      data_id    = options['data_id']
      data       = options['data']

      query = ActiveRecord::Base.send(:sanitize_sql_array, ["UPDATE data_values SET data=data::jsonb || '#{data}'::jsonb WHERE id = ? AND dataset_id = ?", *data_id, *dataset_id])
      ActiveRecord::Base.connection.execute(query)
      dataset
    end

    def delete_data_object(options)
      dataset    = Dataset.select(:id, :data_columns, :data_horizon).find(options['id'])
      data_id    = options['data_id']
      data_value = DataValue.select(:id).find(data_id)
      data_value.destroy
      dataset
    end
  end

  private

    def initialize_options
      @options = DatasetParams.sanitize(@dataset_params)
      @options.keys.each { |k| instance_variable_set("@#{k}", @options[k]) }
    end
end
