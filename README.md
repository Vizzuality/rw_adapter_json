# Resource Watch JSON Service

[![Build Status](https://travis-ci.org/resource-watch/rw_adapter_json.svg?branch=master)](https://travis-ci.org/resource-watch/rw_adapter_json) [![Code Climate](https://codeclimate.com/github/Vizzuality/rw_adapter_json/badges/gpa.svg)](https://codeclimate.com/github/Vizzuality/rw_adapter_json) [![Test Coverage](https://codeclimate.com/github/Vizzuality/rw_adapter_json/badges/coverage.svg)](https://codeclimate.com/github/Vizzuality/rw_adapter_json/coverage)

The world's resources are becoming scarce; at the same time, we aren't providing enough for everyone to live well and prosper. But with trends in mobile phone use, big data processing and global citizenship, we have never been in such a good position to tackle these interrelated issues head on. We just need a way to listen to our planet's signals, together, and broadcast what we learn to those who can use it.

Resource Watch is an open platform for everyone to explore accurate, up-to-date insights about our planet. Anyone can create visualisations or apps, using data from satellites, sensors and social media, to tell stories about the state of our planet. Then, with seamless sharing tools, show the world what you've learnt and inspire action.

For more information visit [resourcewatch.org](http://resourcewatch.org/)

## Installation

Requirements:

* Ruby 2.3.0 [How to install](https://gorails.com/setup/osx/10.10-yosemite)
* PostgreSQL 9.4+ [How to install](http://exponential.io/blog/2015/02/21/install-postgresql-on-mac-os-x-via-brew/)

## Usage

### Natively

First time execute:

    bin/setup

    Or install global dependencies:

    gem install bundler

    bundle install

    cp config/database.yml.sample config/database.yml
    cp env.sample .env

    bundle exec rake db:create
    bundle exec rake db:migrate

To run application:

    bundle exec rails server

### Using Docker

### Requirements for docker

If You are going to use containers, You will need:

- [Docker](https://www.docker.com/)
- [docker-compose](https://docs.docker.com/compose/)

## Executing

Start by checking out the project from github

```
git clone https://github.com/Vizzuality/rw_adapter_json.git
cd rw_adapter_json
```

You can either run the application natively, or inside a docker container.

To setup the project on docker:

```
./service develop
```

To run the tests on docker:

```
./service test
```

## TEST

  Run rspec:

    bin/rspec

## Contributing

1. Fork it!
2. Create your feature branch: `git checkout -b feature/my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin feature/my-new-feature`
5. Submit a pull request :D

### BEFORE CREATING A PULL REQUEST

  Please check all of [these points](https://github.com/resource-watch/rw_adapter_json/blob/master/CONTRIBUTING.md).
