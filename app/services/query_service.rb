require 'typhoeus'
require 'uri'
require 'oj'

class QueryService
  class << self
    def connect_to_query_service(sql_params=nil)
      url = URI.decode("#{ServiceSetting.gateway_url}/convert/sql2FS?sql=#{sql_params}")

      headers = {}
      headers['Accept']         = 'application/json'
      headers['Content-Type']   = 'application/json'
      headers['authentication'] = ServiceSetting.auth_token if ServiceSetting.auth_token.present?

      Typhoeus::Config.memoize = true
      hydra    = Typhoeus::Hydra.new max_concurrency: 100
      @request = ::Typhoeus::Request.new(URI.escape(url), method: :get, headers: headers, followlocation: true)

      @request.on_complete do |response|
        if response.success?
          @data = Oj.load(response.body.force_encoding(Encoding::UTF_8))['data']['attributes']['query'] || Oj.load(response.body.force_encoding(Encoding::UTF_8))
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
  end
end
