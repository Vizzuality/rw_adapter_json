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

  FLUSH_EVERY = 100

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
    dataset = Dataset.select(:id, :data_columns).where(id: @id).first
    dataset.try(:data_columns)
  end

  def data_horizon
    dataset = Dataset.select(:id, :data_horizon).where(id: @id).first
    dataset.try(:data_horizon)
  end

  class << self
    def build_params(options, method)
      dataset_url = options['connector_url'] if options['connector_url'].present?
      data_path   = options['data_path']     if options['data_path'].present?
      params = {}
      data = if options['connector_url'].present? && options['data'].blank?
               ConnectorService.connect_to_provider(dataset_url, data_path, method, options['id'])
             else
               Oj.load(options['data']) if options['data']
             end

      params['data'] = data || []

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

    def gc_rebuild
      ActiveRecord::Base.connection.close
      GC.start(full_mark: false, immediate_sweep: false)
    end

    def concatenate_data(dataset_id, params, date=nil)
      if params['data'].is_a?(Hash) && params['data'].key?(:file_name)
        full_data = YAJI::Parser.new(File.open("#{params['data'][:file_name]}"))
        if params['data'][:path].present?
          path  = params['data'][:path][0].to_s
          path += [params['data'][:path][1]].to_s if params['data'][:path][1].present?
          path += [params['data'][:path][2]].to_s if params['data'][:path][2].present?
          path += [params['data'][:path][3]].to_s if params['data'][:path][3].present?
        else
          path = '/'
        end
        group = []
        batch_size = FLUSH_EVERY

        full_data.each("/#{path}/") do |obj|
          group << obj
          if group.size >= batch_size
            build_data(dataset_id, group, date)
            group = []
          end
        end
        if group.size <= batch_size
          build_data(dataset_id, group, date)
        end
        File.delete("tmp/import/#{dataset_id}.json")
      else
        full_data = params['data']
        full_data = full_data.reject(&:nil?).freeze
        full_data

        full_data.in_groups_of(1000).each do |group|
          group = group.reject(&:nil?)
          build_data(dataset_id, group, date)
        end
      end
    end

    def build_data(dataset_id, group, date)
      group = group.map! { |data| data.each { |key,value| data[key] = value.to_datetime.iso8601 if key.in?(date)  } } if date.present?
      group = group.map! { |data| data.each { |key,value| data[key] = value.gsub("'", "´") if value.is_a?(String) } }
      group = group.map! { |data| data['data_id'].blank? ? data.merge!(data_id: SecureRandom.uuid) : data           }
      group
      query = ActiveRecord::Base.send(:sanitize_sql_array, ["UPDATE datasets SET data=data || '#{group.to_json}' WHERE id = ?", dataset_id])
      ActiveRecord::Base.connection.execute(query)

    end

    def concatenate_data_columns(dataset_id)
      query = ActiveRecord::Base.send(:sanitize_sql_array, ["UPDATE datasets SET data_columns=data::json->0 WHERE id = ?", dataset_id])
      ActiveRecord::Base.connection.execute(query)
      gc_rebuild
    end

    def build_dataset(options)
      params            = build_params(options, 'build_dataset')
      params_for_create = params.except('data').merge(data: [])
      dataset           = Dataset.new(params_for_create)
      date              = options['legend']['date'] if options['legend'].present? && options['legend']['date'].present?

      if dataset.save
        concatenate_data(dataset.id, params, date)
        dataset = Dataset.select(:id, :data_columns).where(id: dataset.id).first
        if options['data_columns'].blank?
          concatenate_data_columns(dataset.id)
          dataset.update_data_columns
        end
      end
      dataset
    end

    def update_dataset(options)
      dataset           = Dataset.select(:id, :data_columns, :data_horizon).where(id: options['id']).first
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
        if options['data_columns'].blank?
          concatenate_data_columns(dataset.id)
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
