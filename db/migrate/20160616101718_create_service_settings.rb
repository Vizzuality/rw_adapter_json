class CreateServiceSettings < ActiveRecord::Migration[5.0]
  def change
    create_table :service_settings, id: :uuid do |t|
      t.string  :name
      t.string  :token
      t.string  :url
      t.boolean :listener

      t.timestamps
    end

    add_index :service_settings, ['name'], name: 'index_service_settings_on_name', unique: true
  end
end
