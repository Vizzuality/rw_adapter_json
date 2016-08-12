class ConnectorSerializer < ApplicationSerializer
  attributes :data

  def data
    object.data(@query_filter)
  end

  def initialize(object, options)
    super
    @query_filter = options[:query_filter]
  end
end
