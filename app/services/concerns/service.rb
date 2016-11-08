# frozen_string_literal: true
module Service
  SERVICE_URL   = ENV.fetch('API_GATEWAY_URL') { ServiceSetting.gateway_url.freeze }
  SERVICE_TOKEN = ENV.fetch('API_GATEWAY_URL') { ServiceSetting.auth_token.freeze  }
end
