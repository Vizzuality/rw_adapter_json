FROM ruby:2.3.0
RUN apt-get update -qq && apt-get install -y build-essential nodejs npm nodejs-legacy postgresql-client

RUN mkdir /rw_adapter_json

WORKDIR /tmp
COPY Gemfile Gemfile
COPY Gemfile.lock Gemfile.lock
RUN bundle install

ADD . /rw_adapter_json

WORKDIR /rw_adapter_json

EXPOSE 3010

ENTRYPOINT ["./entrypoint.sh"]
