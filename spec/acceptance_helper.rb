require 'rails_helper'

def json
  JSON.parse(response.body)
end

def json_main
  JSON.parse(response.body)
end
