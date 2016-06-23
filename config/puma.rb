workers Integer(ENV['WEB_CONCURRENCY'] || 2)
# Min and Max threads per worker
threads_count = Integer(ENV['RAILS_MAX_THREADS'] || 5)
threads threads_count, threads_count

rackup DefaultRackup
port        ENV['PORT']     || 3010
environment ENV['RACK_ENV'] || 'development'

preload_app!

# daemonize true

# Set master PID and state locations
# pidfile    'tmp/pids/puma.pid'
# state_path 'tmp/pids/puma.state'

on_worker_boot do
  ActiveRecord::Base.establish_connection
end
