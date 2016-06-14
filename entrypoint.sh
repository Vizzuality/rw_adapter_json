#!/bin/bash
set -e

case "$1" in
    develop)
        echo "Running Development Server"
        rm -f tmp/pids/puma.pid
        exec ./server start develop
        ;;
    test)
        echo "Running Test"
        rm -f tmp/pids/puma.pid
        exec rspec
        ;;
    start)
        echo "Running Start"
        rm -f tmp/pids/puma.pid
        exec ./server start production
        ;;
    *)
        exec "$@"
esac