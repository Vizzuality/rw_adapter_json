module V1
  class InfoController < ApplicationController
    def info
      @service = ServiceSetting.save_gateway_settings(params)
      if @service
        @docs = Oj.load(File.read("lib/files/service_#{ENV['RAILS_ENV']}.json"))
        render json: @docs
      else
        render json: { success: false, message: 'Missing url and token params' }, status: 422
      end
    end

    def ping
      render json: { success: true, message: 'RW Tags online' }, status: 200
    end
  end
end
