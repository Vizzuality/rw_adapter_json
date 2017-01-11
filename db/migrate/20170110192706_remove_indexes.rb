class RemoveIndexes < ActiveRecord::Migration[5.0]
  def change
    remove_index :datasets, name: 'index_datasets_on_data'
    remove_index :datasets, name: 'index_datasets_on_data_columns'
    remove_index :service_settings, name: 'index_service_settings_on_name'
  end
end
