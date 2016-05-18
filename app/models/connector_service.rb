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
      url    = URI.decode("#{ENV['API_DATASET_META_URL']}/#{dataset_id}")

      @c = Curl::Easy.http_put(URI.escape(url), Oj.dump(params)) do |curl|
        curl.headers['Accept']       = 'application/json'
        curl.headers['Content-Type'] = 'application/json'
      end
    end

    def connect_to_provider(connector_url, data_path)
      url = URI.decode(connector_url)

      @c = Curl::Easy.http_get(URI.escape(url)) do |curl|
        curl.headers['Accept']       = 'application/json'
        curl.headers['Content-Type'] = 'application/json'
      end

      Oj.load(@c.body_str.force_encoding(Encoding::UTF_8))[data_path] || Oj.load(@c.body_str.force_encoding(Encoding::UTF_8))
    end
  end
end
