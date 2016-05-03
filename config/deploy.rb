require 'mina/bundler'
require 'mina/rails'
require 'mina/git'
require 'mina/rvm'

set :domain, 'ubuntu@ec2-52-23-163-254.compute-1.amazonaws.com'
set :deploy_to, '/home/ubuntu/rw_adapter_json'
set :repository, 'https://github.com/Vizzuality/rw_adapter_json.git'
set :branch, 'master'
set :rails_env, 'production'
set :application, 'rw_adapter_json'
set :foreman_sudo, 'ubuntu'

set :shared_paths, ['config/database.yml', 'config/secrets.yml', 'config/sidekiq.yml', 'log', '.env']

task :environment do
  invoke :'rvm:use[ruby-2.3.0@rw_adapter_json]'
end

task setup: :environment do
  queue! %[mkdir -p "#{deploy_to}/#{shared_path}/log"]
  queue! %[chmod g+rx,u+rwx "#{deploy_to}/#{shared_path}/log"]

  queue! %[mkdir -p "#{deploy_to}/#{shared_path}/config"]
  queue! %[chmod g+rx,u+rwx "#{deploy_to}/#{shared_path}/config"]

  queue! %[touch "#{deploy_to}/#{shared_path}/config/database.yml"]
  queue! %[touch "#{deploy_to}/#{shared_path}/config/secrets.yml"]
  queue  %[echo "-----> Be sure to edit '#{deploy_to}/#{shared_path}/config/database.yml' and 'secrets.yml'."]
  queue! %[touch "#{deploy_to}/#{shared_path}/.env"]
  queue  %[echo "-----> Be sure to edit '/.env'."]

  if repository
    repo_host = repository.split(%r{@|://}).last.split(%r{:|\/}).first
    repo_port = /:([0-9]+)/.match(repository) && /:([0-9]+)/.match(repository)[1] || '22'

    queue %[
      if ! ssh-keygen -H  -F #{repo_host} &>/dev/null; then
        ssh-keyscan -t rsa -p #{repo_port} -H #{repo_host} >> ~/.ssh/known_hosts
      fi
    ]
  end
end

desc "Deploys the current version to the server."
task deploy: :environment do
  to :before_hook do
    # Put things to run locally before ssh
  end
  deploy do
    invoke :'git:clone'
    invoke :'deploy:link_shared_paths'
    invoke :'bundle:install'
    invoke :'rails:db_migrate'
    invoke :'deploy:cleanup'
    invoke :'foreman:export'

    to :launch do
      invoke 'foreman:restart'
      invoke 'service:start'
    end
  end
end

desc "Registering the application services"
task register_service: :environment do
  invoke 'service:register'
end

desc "Restarting the application services"
task start_service: :environment do
  invoke 'service:start'
end

set_default :foreman_app,  lambda { application }
set_default :foreman_user, lambda { user }
set_default :foreman_log,  lambda { "#{deploy_to!}/#{shared_path}/log" }

namespace :foreman do
  desc 'Export the Procfile to Ubuntu upstart scripts'
  task :export do
    export_cmd = "rvmsudo bundle exec foreman export upstart /etc/init -a #{foreman_app} -u #{foreman_user} -l #{foreman_log}"

    queue %{
      echo "-----> Exporting foreman procfile for #{foreman_app}"
      #{echo_cmd %[cd #{deploy_to!}/#{current_path!} ; #{export_cmd}]}
    }
  end

  desc "Start the application services"
  task :start do
    queue %{
      echo "-----> Starting #{foreman_app} services"
      #{echo_cmd %[sudo start #{foreman_app}]}
    }
  end

  desc "Stop the application services"
  task :stop do
    queue %{
      echo "-----> Stopping #{foreman_app} services"
      #{echo_cmd %[sudo stop #{foreman_app}]}
    }
  end

  desc "Restart the application services"
  task :restart do
    queue %{
      echo "-----> Restarting #{foreman_app} services"
      #{echo_cmd %[sudo start #{foreman_app} || sudo restart #{foreman_app}]}
    }
  end
end

namespace :service do
  desc "Registering the application services"
  task :register do
    queue %{
      echo "-----> Registering #{foreman_app} services"
      #{echo_cmd %[cd #{deploy_to!}/#{current_path!} ; ./server register-service]}
    }
  end

  desc "Restarting the application services"
  task :start do
    queue %{
      echo "-----> Restarting #{foreman_app} services"
      #{echo_cmd %[cd #{deploy_to!}/#{current_path!} ; ./server start production]}
    }
  end
end
