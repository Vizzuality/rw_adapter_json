# frozen_string_literal: true
require File.expand_path('../boot', __FILE__)

require "rails"
# Pick the frameworks you want:
require "active_model/railtie"
require "active_job/railtie"
require "active_record/railtie"
require "action_controller/railtie"
# require "action_mailer/railtie"
# require "action_view/railtie"
# require "action_cable/engine"
# require "sprockets/railtie"
# require "rails/test_unit/railtie"

# Require the gems listed in Gemfile, including any gems
# you've limited to :test, :development, :staging, or :production.
Bundler.require(*Rails.groups)
GC::Profiler.enable

module RwAdapterJson
  class Application < Rails::Application
    # config.active_record.schema_format = :sql
    # Settings in config/environments/* take precedence over those specified here.
    # Application configuration should go into files in config/initializers
    # -- all .rb files in that directory are automatically loaded.

    # Only loads a smaller set of middleware suitable for API only apps.
    # Middleware like session, flash, cookies can be added back manually.
    # Skip views, helpers and assets when generating a new resource.
    config.api_only = true
    config.autoload_paths += Dir[Rails.root.join('app', 'models', '{**}')]

    config.middleware.insert_before(Rack::Sendfile, Rack::Chunked)

    # config.middleware.use Rack::Attack

    config.generators do |g|
      g.orm :active_record, primary_key_type: :uuid
      g.test_framework :rspec,
        fixtures: true,
        routing_specs: true,
        controller_specs: true,
        request_specs: true
    end

    config.host = ENV.fetch('REDIS_PORT_6379_TCP_ADDR') { 'localhost' }
    config.port = ENV.fetch('REDIS_PORT_6379_TCP_PORT') { 6379        }

    config.redis_url = "redis://#{config.host}:#{config.port}/0/cache"
  end
end
