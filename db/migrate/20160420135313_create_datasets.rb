class CreateDatasets < ActiveRecord::Migration[5.0]
  def change
    create_table :datasets, id: :uuid, default: 'uuid_generate_v4()', force: true do |t|
      t.jsonb   :data_columns, default: '{}'
      t.jsonb   :data,         default: '[]'
      t.integer :data_horizon, default: 0

      t.timestamps
    end

    add_index :datasets, :data_columns, using: :gin
    add_index :datasets, :data,         using: :gin
  end
end
