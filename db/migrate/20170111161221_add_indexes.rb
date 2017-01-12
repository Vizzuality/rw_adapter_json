class AddIndexes < ActiveRecord::Migration[5.0]
  def change
    add_index :datasets, :data_columns, using: :gin
    add_index :datasets, :data,         using: :gin
  end
end
