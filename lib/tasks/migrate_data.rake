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
    Dataset.all.each do |dataset|
      data = YAJI::Parser.new(dataset.data.to_json)
      data.each("/") do |obj|
        unless DataValue.select(:id).where(id: dataset['data_id']).first.present?
          group << DataValue.new(id: dataset['data_id'], dataset_id: dataset.id, data: obj)
          if group.size >= batch_size
            DataValue.import group
            group = []
          end
        end
      end
      if group.size <= batch_size
        DataValue.import group if group.present?
        group = []
      end
    end
    puts 'All dataset values imported'
  end
end
