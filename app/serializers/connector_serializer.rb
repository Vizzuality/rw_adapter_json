class ConnectorSerializer < ApplicationSerializer
  attributes :rows

  def rows
    object.data(@query_filter)
  end

  def initialize(object, options)
    super
    @query_filter = options[:query_filter]
  end
end
