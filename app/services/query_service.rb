require 'curb'
require 'uri'
require 'oj'

class QueryService
  class << self
    def connect_to_query_service(sql_params=nil)
      url = URI.decode("#{ServiceSetting.gateway_url}/convert/sql2FS?sql=#{sql_params}")

      @c = Curl::Easy.http_get(URI.escape(url)) do |curl|
        curl.headers['Accept']         = 'application/json'
        curl.headers['Content-Type']   = 'application/json'
        curl.headers['authentication'] = ServiceSetting.auth_token if ServiceSetting.auth_token.present?
      end
      @c.perform

      Oj.load(@c.body_str.force_encoding(Encoding::UTF_8))['data']['attributes']['query'] || Oj.load(@c.body_str.force_encoding(Encoding::UTF_8))
    end
  end
end
