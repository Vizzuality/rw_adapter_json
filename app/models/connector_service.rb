require 'typhoeus'
require 'uri'
require 'oj'

class ConnectorService
  class << self
    def establish_connection(url, method, params={})
      @request = ::Typhoeus::Request.new(URI.escape(url), method: method, headers: { 'Accept' => 'application/json' }, params: params)
      @request.run
    end

    def connect_to_dataset_service(dataset_id, status)
      status   = case status
                 when 'saved' then 1
                 when 'deleted' then 3
                 else 2
                 end
      params   = { dataset: { dataset_attributes: { status: status } } }
      url      = URI.decode("#{ENV['API_DATASET_META_URL']}/#{dataset_id}")

      establish_connection(url, 'put', params)
    end

    def connect_to_provider(connector_url, data_path)
      url = URI.decode(connector_url)

      establish_connection(url, 'get')

      Oj.load(@request.response.body.force_encoding(Encoding::UTF_8))[data_path] || Oj.load(@request.response.body.force_encoding(Encoding::UTF_8))
    end
  end
end
