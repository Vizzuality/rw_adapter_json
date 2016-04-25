module RequestOnComplete
  extend ActiveSupport::Concern

  included do
    def self.request_on_complete(request)
      request.on_complete do |response|
        if response.success?
          # cool
        elsif response.timed_out?
          'got a time out'
        elsif response.code == 0
          response.return_message
        else
          'HTTP request failed: ' + response.code.to_s
        end
      end
    end
  end

  class_methods do
  end
end
