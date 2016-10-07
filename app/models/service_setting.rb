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
      first.try(:token) || ENV['API_GATEWAY_TOKEN']
    end

    def gateway_url
      first.try(:url) || ENV['API_GATEWAY_URL']
    end
  end
end
