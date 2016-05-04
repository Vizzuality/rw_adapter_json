if ENV["REDISCLOUD_URL"]
  uri    = URI.parse(ENV['REDISCLOUD_URL'])
  $redis = Redis.new(host: uri.host, port: uri.port, password: uri.password)
else
  host     = ENV.fetch('REDIS_HOST')     { 'localhost' }
  port     = ENV.fetch('REDIS_PORT')     { 6379 }
  password = ENV.fetch('REDIS_PASSWORD') { '' }
  $redis   = Redis.new(host: host, port: port, password: password)
end
