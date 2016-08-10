class ConnectorFieldsSerializer < ApplicationSerializer
  attributes :tableName, :fields

  def fields
    object.data_columns
  end

  def tableName
    object.try(:table_name)
  end
end
