#!/bin/bash
set -e
ps -fea

if [ "$1" = 'run' ]; then
  bin/rails server --port 3010 --binding 0.0.0.0
else
    exec "$@"
fi
