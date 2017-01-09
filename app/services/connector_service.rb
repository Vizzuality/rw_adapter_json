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

    def connect_to_provider(connector_url, data_path, method=nil)
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
      @request = Typhoeus::Request.new(URI.escape(url), method: :get, headers: headers, followlocation: true)

      if method == 'build_dataset'
        downloaded_file_name = "tmp/import/#{Time.now.to_s.parameterize}.json"
        downloaded_file      = File.open(downloaded_file_name, 'wb')
        @request.on_headers do |response|
          if response.code != 200
            raise 'Request failed'
          end
        end
        @request.on_body do |chunk|
          downloaded_file.write(chunk)
        end
      end

      @request.on_complete do |response|
        downloaded_file.close if method == 'build_dataset'
        if response.success?
          if method == 'build_dataset'
            downloaded_file.close
            @data = { file_name: downloaded_file_name, path: path, path_size: path_size }
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

    def integer?(str)
      /\A[+-]?\d+\z/ === str
    end
  end
end
