require 'acceptance_helper'

module V1
  describe 'Create Service Settings', type: :request do
    context 'For none existing settings' do
      it 'Allows to create service token and gateway url' do
        get "/info?token=3123123der324eewr434ewr4324&url=http://192.168.99.100:8000"

        expect(status).to eq(200)
        expect(ServiceSetting.first.token).to eq('3123123der324eewr434ewr4324')
        expect(ServiceSetting.first.url).to   eq('http://192.168.99.100:8000')
      end
    end

    context 'For existing settings' do
      let!(:settings) { ServiceSetting.create(name: 'api-gateway', listener: true, token: '3123123der324eewr434ewr4324', url: 'http://192.168.99.100:8000') }

      it 'Allows to update service token and gateway url' do
        get "/info?token=my-new-token&url=http://my-new-url.com"

        expect(status).to eq(200)
        expect(ServiceSetting.all.size).to    eq(1)
        expect(ServiceSetting.first.id).to    eq(settings.id)
        expect(ServiceSetting.first.token).to eq('my-new-token')
        expect(ServiceSetting.first.url).to   eq('http://my-new-url.com')
      end
    end

    context 'For missing params' do
      it 'Allows to create service token and gateway url' do
        get "/info"

        expect(status).to eq(422)
        expect(ServiceSetting.all.size).to eq(0)
      end
    end
  end
end