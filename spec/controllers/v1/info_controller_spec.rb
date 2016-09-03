require 'rails_helper'

RSpec.describe V1::InfoController, type: :controller do
  describe 'Get info' do
    it 'Info responds 200' do
      get :info, params: { token: '3123123der324eewr434ewr4324', url: 'http://192.168.99.100:8000' }
      expect(response.status).to eq 200
    end

    it 'Info responds 422' do
      get :info
      expect(response.status).to eq 422
    end
  end

  describe 'Ping' do
    it 'Ping responds 200' do
      get :ping
      expect(response.status).to eq 200
    end
  end
end
