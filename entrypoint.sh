#!/bin/bash
set -e

case "$1" in
    develop)
        echo "Running Development Server"
        gem install bundler --conservative
        bundle install --without=test,production

        bundle exec rake db:exists RAILS_ENV=development

        export SECRET_KEY_BASE=$(rake secret)

        exec ./server start develop
        ;;
    test)
        echo "Running Test"
        gem install bundler
        bundle install --without=development,production

        bundle exec rake db:exists RAILS_ENV=test

        export SECRET_KEY_BASE=$(rake secret)

        exec rspec
        ;;
    start)
        echo "Running Start"
        gem install bundler
        bundle install --without=development,test

        bundle exec rake db:exists RAILS_ENV=production

        export SECRET_KEY_BASE=$(rake secret)
        mkdir -p tmp/pids
        mkdir log

        exec ./server start production
        ;;
    *)
        exec "$@"
esac
