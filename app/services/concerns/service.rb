# frozen_string_literal: true
module Service
  SERVICE_URL   = ENV.fetch('GATEWAY_URL')   { ServiceSetting.gateway_url.freeze }
  SERVICE_TOKEN = ENV.fetch('GATEWAY_TOKEN') { ServiceSetting.auth_token.freeze  }
end
