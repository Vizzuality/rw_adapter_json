# frozen_string_literal: true
module V1
  class InfoController < ApplicationController
    def info
      @service = ServiceSetting.save_gateway_settings(params)
        @docs = Oj.load(File.read("lib/files/service_#{ENV['RAILS_ENV']}.json"))
        render json: @docs
    end

    def ping
      render json: { success: true, message: 'RW Tags online' }, status: 200
    end
  end
end
