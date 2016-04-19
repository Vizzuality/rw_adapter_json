class ConnectorSerializer < ActiveModel::Serializer
  attributes :id, :connector_name, :provider, :format, :connector_path, :clone_url, :data_attributes, :data

  def clone_url
    data = {}
    data['http_method'] = 'POST'
    data['url']         = "#{URI.parse(clone_uri)}"
    data['body']        = body_params
    data
  end

  def data
    object.data(@options[:query_filter])
  end

  def uri
    "#{@options[:uri]['api_gateway_url']}#{@options[:uri]['full_path']}"
  end

  def clone_uri
    "#{@options[:uri]['api_gateway_url']}/datasets/#{object.id}/clone"
  end

  def body_params
    {
      "dataset" => {
        "dataset_url" => "#{URI.parse(uri)}"
      }
    }
  end
end
