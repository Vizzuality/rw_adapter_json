# frozen_string_literal: true
require 'curb'
require 'typhoeus'
require 'uri'
require 'oj'
require 'yajl'

group = []
batch_size = 1000

namespace :migrate do
  desc 'Migrate data from data array to values'
  task data: :environment do
    puts 'Migrate Dataset values...'
    Dataset.where.not(data_horizon: 3).each_with_index do |dataset, i|
      data = YAJI::Parser.new(dataset.data.to_json)
      data.each("/") do |obj|
        unless DataValue.select(:id).where(id: obj['data_id']).first.present?
          group << DataValue.new(id: obj['data_id'], dataset_id: dataset.id, data: obj)
          if group.size >= batch_size
            DataValue.import group
            GC.start(full_mark: false, immediate_sweep: false)
            group = []
          end
        end
      end
      if group.size <= batch_size
        DataValue.import group if group.present?
        GC.start(full_mark: false, immediate_sweep: false)
        group = []
      end
      puts "#{i}... Dataset: #{dataset.id} migrated"
    end
    puts 'All dataset values imported'
  end
end

namespace :id_fixes do
  desc 'Fixes for data data_id'
  task data: :environment do
    puts 'Save Dataset values...'
    DataValue.all.each do |data_value|
      if data_value.id != data_value.data['data_id']
        ActiveRecord::Base.transaction do
          query = ActiveRecord::Base.send(:sanitize_sql_array, ["UPDATE data_values SET data=data::jsonb || {'data_id': '#{data_value.id}'}::jsonb"])
          ActiveRecord::Base.connection.execute(query)
        end
      end
      GC.start(full_mark: false, immediate_sweep: false)
    end
    puts 'End'
  end
end

namespace :destroy do
  desc 'Fixes for data data_id'
  task data: :environment do
    puts 'Delete Dataset values...'
    Dataset.all.each_with_index do |dataset, i|
      dataset.data_values.destroy_all
      GC.start(full_mark: false, immediate_sweep: false)
      puts "#{i}... Dataset: #{dataset.id} data deleted"
    end
    puts 'End'
  end
end
