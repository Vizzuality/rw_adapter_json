class CreateDataValues < ActiveRecord::Migration[5.0]
  def change
    create_table :data_values, id: :uuid do |t|
      t.uuid  :dataset_id, index: true
      t.jsonb :data, default: '{}'

      t.timestamps
    end

    add_index :data_values, :data, using: :gin
  end
end
