# frozen_string_literal: true
# == Schema Information
#
# Table name: service_settings
#
#  id         :uuid             not null, primary key
#  name       :string
#  token      :string
#  url        :string
#  listener   :boolean
#  created_at :datetime         not null
#  updated_at :datetime         not null
#

class ServiceSetting < ApplicationRecord
  class << self
    def save_gateway_settings(options=nil)
      return false unless options['token'].present? && options['url'].present?

      service = ServiceSetting.first_or_initialize(name: 'api-gateway')
      service.update_attributes(listener: true, url: options[:url], token: options[:token])
    end

    def auth_token
      ENV['GATEWAY_TOKEN'] || first.try(:token)
    end

    def gateway_url
      ENV['GATEWAY_URL'] || first.try(:url)
    end
  end
end
