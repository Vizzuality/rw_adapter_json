require 'curb'
require 'uri'
require 'oj'

class ConnectorService
  class << self
    def connect_to_dataset_service(dataset_id, status)
      status = case status
               when 'saved' then 1
               when 'deleted' then 3
               else 2
               end

      params = { dataset: { dataset_attributes: { status: status } } }
      url    = URI.decode("#{ServiceSetting.gateway_url}/datasets/#{dataset_id}")

      @c = Curl::Easy.http_put(URI.escape(url), Oj.dump(params)) do |curl|
        curl.headers['Accept']         = 'application/json'
        curl.headers['Content-Type']   = 'application/json'
        curl.headers['authentication'] = ServiceSetting.auth_token if ServiceSetting.auth_token.present?
      end
    end

    def connect_to_provider(connector_url, data_path)
      data_path = data_path.to_i if integer? data_path
      data_path = nil            if data_path.include?('root_path')
      url = URI.decode(connector_url)

      @c = Curl::Easy.http_get(URI.escape(url)) do |curl|
        curl.headers['Accept']       = 'application/json'
        curl.headers['Content-Type'] = 'application/json'
      end

      if data_path.present?
        Oj.load(@c.body_str.force_encoding(Encoding::UTF_8))[data_path]
      else
        Oj.load(@c.body_str.force_encoding(Encoding::UTF_8))
      end
    end

    def integer?(str)
      /\A[+-]?\d+\z/ === str
    end
  end
end
