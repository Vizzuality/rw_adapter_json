# frozen_string_literal: true
require 'curb'
require 'uri'
require 'oj'

module ConnectorService
  class << self
    def connect_to_dataset_service(dataset_id, status)
      status = case status
               when 'saved' then 1
               when 'deleted' then 3
               else 2
               end

      params = { dataset: { status: status } }
      url    = URI.decode("#{Service::SERVICE_URL}/dataset/#{dataset_id}")

      @c = Curl::Easy.http_put(URI.escape(url), Oj.dump(params)) do |curl|
        curl.headers['Accept']         = 'application/json'
        curl.headers['Content-Type']   = 'application/json'
        curl.headers['authentication'] = Service::SERVICE_TOKEN
      end
      @c.perform
    end

    def connect_to_provider(connector_url, data_path)
      if integer? data_path
        path = data_path.to_i
      elsif data_path.include?('root_path')
        path = nil
      else
        path      = data_path.split(',')
        path_size = path.size
      end
      url = URI.decode(connector_url)

      @c = Curl::Easy.http_get(URI.escape(url)) do |curl|
        curl.headers['Accept']       = 'application/json'
        curl.headers['Content-Type'] = 'application/json'
      end
      @c.perform

      if path.present? && path_size.positive?
        data = Oj.load(@c.body_str.force_encoding(Encoding::UTF_8))[path[0]]
        data = data[path[1]] if path[1].present?
        data = data[path[2]] if path[2].present?
        data = data[path[3]] if path[3].present?
        data
      else
        Oj.load(@c.body_str.force_encoding(Encoding::UTF_8))
      end
    end

    def integer?(str)
      /\A[+-]?\d+\z/ === str
    end
  end
end
