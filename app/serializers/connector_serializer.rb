class ConnectorSerializer < ActiveModel::Serializer
  attributes :clone_url, :fields, :data

  def clone_url
    data = {}
    data['http_method'] = 'POST'
    data['url']         = "#{URI.parse(clone_uri)}"
    data['body']        = body_params
    data
  end

  def data
    object.data(@query_filter)
  end

  def fields
    object.data_columns
  end

  def uri
    "#{@uri['api_gateway_url']}#{@uri['full_path']}"
  end

  def clone_uri
    "#{@uri['api_gateway_url']}/datasets/#{object.id}/clone"
  end

  def body_params
    {
      "dataset" => {
        "dataset_url" => "#{URI.parse(uri)}"
      }
    }
  end

  def initialize(object, options)
    super
    @query_filter = options[:query_filter]
    @uri          = options[:uri]
  end
end
