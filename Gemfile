# frozen_string_literal: true
source 'https://rubygems.org'

ruby '2.3.1'

gem 'rails', '~> 5.0.0', '< 5.1'

gem 'activerecord-import'
gem 'pg', '~> 0.18'

gem 'active_model_serializers', '~> 0.10.2'
gem 'curb', require: false
gem 'oj'
gem 'oj_mimic_json'
gem 'typhoeus', require: false
gem 'yaji'
gem 'yajl-ruby', require: 'yajl'

group :development, :test do
  gem 'byebug'
  gem 'dotenv-rails'
  gem 'faker'
  gem 'rspec-activejob'
  gem 'rspec-rails', '~> 3.5.1'
  gem 'rubocop', require: false
  gem 'webmock'
end

group :development do
  gem 'annotate'
  gem 'brakeman', require: false
  gem 'listen', '~> 3.0.5'
  gem 'pry-rails'
  gem 'spring'
  gem 'spring-watcher-listen', '~> 2.0.0'
end

group :test do
  gem 'bullet'
  gem 'codeclimate-test-reporter', require: nil
  gem 'database_cleaner'
  gem 'timecop'
end

# Server
gem 'newrelic_rpm'
gem 'puma'
gem 'rack-attack'
gem 'rack-cors'
gem 'redis', '~> 3.2'
gem 'redis-namespace'
gem 'redis-rails'
gem 'sidekiq'
gem 'tzinfo-data'
