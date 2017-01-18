require 'rails_helper'

RSpec.describe V1::InfoController, type: :controller do
  describe 'Get info' do
    it 'Info responds 200' do
      get :info
      expect(response.status).to eq 200
    end
  end

  describe 'Ping' do
    it 'Ping responds 200' do
      get :ping
      expect(response.status).to eq 200
    end
  end
end
