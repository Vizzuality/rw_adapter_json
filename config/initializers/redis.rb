host     = ENV.fetch('REDIS_PORT_6379_TCP_ADDR')   { 'localhost' }
port     = ENV.fetch('REDIS_1_PORT_6379_TCP_PORT') { 6379 }
password = ENV.fetch('REDIS_PASSWORD')             { '' }
$redis   = Redis.new(host: host, port: port, password: password)
