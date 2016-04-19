class Dataset < ApplicationRecord
  self.table_name = :datasets

  include ReadOnlyModel
  include NullAttributesRemover

  belongs_to :dateable, polymorphic: true

  def self.execute_data_query(sql_org)
    sql = sanitize_sql(sql_org)
    connection.select_all(sql)
  end
end
