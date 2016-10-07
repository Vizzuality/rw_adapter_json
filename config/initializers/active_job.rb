# frozen_string_literal: true
# Use Sidekiq for development and production config file in config/sidekiq.yml
ActiveJob::Base.queue_adapter = :sidekiq unless Rails.env.test?
ActiveJob::Base.queue_adapter = :test    if Rails.env.test?
