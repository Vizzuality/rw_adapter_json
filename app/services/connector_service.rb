# frozen_string_literal: true
require 'curb'
require 'typhoeus'
require 'uri'
require 'oj'
require 'yajl'

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

      headers = {}
      headers['Accept']       = 'application/json'
      headers['Content-Type'] = 'application/json'

      Typhoeus::Config.memoize = true
      hydra    = Typhoeus::Hydra.new max_concurrency: 100
      @request = Typhoeus::Request.new(URI.escape(url), method: :get, headers: headers)

      @request.on_complete do |response|
        if response.success?
          if path.present? && path_size.positive?
            data  = response_processor(path, response)
            data  = data[path[1]] if path[1].present?
            data  = data[path[2]] if path[2].present?
            data  = data[path[3]] if path[3].present?
            @data = data
          else
            parser = Yajl::Parser.new
            @data  = parser.parse(response.body.force_encoding(Encoding::UTF_8))
          end
        elsif response.timed_out?
          @data = 'got a time out'
        elsif response.code.zero?
          @data = response.return_message
        else
          @data = Oj.load(response.body)
        end
      end
      hydra.queue @request
      hydra.run
      @data
    end

    def response_processor(path, response)
      batch      = []
      batch_size = 10000
      parser     = YAJI::Parser.new(response.body.force_encoding(Encoding::UTF_8))
      data_set   = []

      parser.each("/#{path[0]}/") do |obj|
        batch << obj
        if batch.size >= batch_size
          data_set = data_set | batch
          batch    = []
        end
      end
      if batch.size <= batch_size
        data_set = data_set | batch
      end
      data_set
    end

    def integer?(str)
      /\A[+-]?\d+\z/ === str
    end
  end
end
