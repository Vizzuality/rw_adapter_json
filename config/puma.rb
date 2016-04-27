# Change to match your CPU core count
# workers 1

# Min and Max threads per worker
threads 0,5

rackup DefaultRackup
# Default to 3000
port ENV.fetch('RW_JSON_PORT') { 3010 }
# Default to production
environment ENV.fetch('RAILS_ENV') { 'development' }

daemonize true

# Set master PID and state locations
pidfile    'tmp/pids/puma.pid'
state_path 'tmp/pids/puma.state'

on_worker_boot do
  ActiveSupport.on_load(:active_record) do
    config = ActiveRecord::Base.configurations[Rails.env] || Rails.application.config.database_configuration[Rails.env]
    config['pool'] = ENV['MAX_THREADS'] || 5
    ActiveRecord::Base.establish_connection(config)
  end
end
