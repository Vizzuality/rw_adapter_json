require 'rails_helper'

RSpec.describe JsonConnector, type: :model do
  let!(:data_id) { 'fd2a6bab-5697-404b-9cf9-5905bba17711' }

  let!(:data_columns) {{
                        "pcpuid": {
                          "type": "string"
                        },
                        "the_geom": {
                          "type": "geometry"
                        },
                        "cartodb_id": {
                          "type": "number"
                        },
                        "the_geom_webmercator": {
                          "type": "geometry"
                        }
                      }}

  let!(:data) {[{
                  "pcpuid": "350558",
                  "the_geom": "0101000020E610000000000000786515410000000078651541",
                  "cartodb_id": 2,
                  "data_id": "fd2a6bab-5697-404b-9cf9-5905bba17712"
                },
                {
                  "pcpuid": "350659",
                  "the_geom": "0101000020E6100000000000000C671541000000000C671541",
                  "cartodb_id": 3,
                  "data_id": "fd2a6bab-5697-404b-9cf9-5905bba17713"
                },
                {
                  "pcpuid": "481347",
                  "the_geom": "0101000020E6100000000000000C611D41000000000C611D41",
                  "cartodb_id": 4,
                  "data_id": "fd2a6bab-5697-404b-9cf9-5905bba17714"
                },
                {
                  "pcpuid": "120171",
                  "the_geom": "0101000020E610000000000000B056FD4000000000B056FD40",
                  "cartodb_id": 5,
                  "data_id": "fd2a6bab-5697-404b-9cf9-5905bba17715"
                },
                {
                  "pcpuid": "500001",
                  "the_geom": "0101000020E610000000000000806EF84000000000806EF840",
                  "cartodb_id": 1,
                  "data_id": data_id
              }]}

  let!(:dataset) {
    dataset = Dataset.create!(data_columns: data_columns)
    dataset.data_values.create(id: 'fd2a6bab-5697-404b-9cf9-5905bba17719', data: data.last)
    dataset
  }

  let!(:dataset_id) { dataset.id }

  let!(:options_build) {
    options = {}
    options['id']           = 'fd2a6bab-5697-404b-9cf9-5905bba17755'
    options['data_columns'] = Oj.dump(data_columns)
    options['data']         = Oj.dump(data)
    options
  }

  let!(:options_update) {
    options = {}
    options['id']           = dataset_id
    options['data_columns'] = Oj.dump(data_columns)
    options['data']         = Oj.dump(data)
    options
  }

  let!(:options_overwrite) {
    options = {}
    options['id']   = dataset_id
    options['data'] = Oj.dump([{ "my_new_key": "Data overwrite" }])
    options
  }

  let!(:options_update_data) {
    options = {}
    options['id']      = dataset_id
    options['data_id'] = 'fd2a6bab-5697-404b-9cf9-5905bba17719'
    options['data']    = Oj.dump({ "the_geom": "update geom" })
    options
  }

  let!(:options_delete_data) {
    options = {}
    options['id']      = dataset_id
    options['data_id'] = 'fd2a6bab-5697-404b-9cf9-5905bba17719'
    options
  }

  it 'Build dataset' do
    JsonConnector.build_dataset(options_build)
    dataset = Dataset.find('fd2a6bab-5697-404b-9cf9-5905bba17755')
    expect(dataset.data_values.count).to  eq(5)
    expect(dataset.data_columns).to eq({"pcpuid"=>{"type"=>"string"}, "the_geom"=>{"type"=>"geometry"}, "cartodb_id"=>{"type"=>"number"}, "the_geom_webmercator"=>{"type"=>"geometry"}})
  end

  it 'Update dataset concatenate data' do
    JsonConnector.update_dataset(options_update)
    dataset = Dataset.find(dataset_id)
    expect(dataset.data_values.size).to eq(6)
    expect(dataset.data_columns).to eq({"pcpuid"=>{"type"=>"string"}, "the_geom"=>{"type"=>"geometry"}, "cartodb_id"=>{"type"=>"number"}, "the_geom_webmercator"=>{"type"=>"geometry"}})
  end

  it 'Overwrite dataset data' do
    JsonConnector.overwrite_data(options_overwrite)
    dataset = Dataset.find(dataset_id)
    expect(dataset.data_values.size).to eq(1)
    expect(dataset.data_columns).to eq({"data_id"=>{"type"=>"string"}, "my_new_key"=>{"type"=>"string"}})
  end

  it 'Update dataset update specific data object' do
    JsonConnector.update_data_object(options_update_data)
    dataset = Dataset.find(dataset_id)
    expect(dataset.data_values.size).to eq(1)
    expect(dataset.data_values.pluck(:data).first['the_geom']).to eq('update geom')
    expect(dataset.data_columns).to eq({"pcpuid"=>{"type"=>"string"}, "the_geom"=>{"type"=>"geometry"}, "cartodb_id"=>{"type"=>"number"}, "the_geom_webmercator"=>{"type"=>"geometry"}})
  end

  it 'Update dataset delete specific data object' do
    JsonConnector.delete_data_object(options_delete_data)
    dataset = Dataset.find(dataset_id)
    expect(dataset.data_values.size).to eq(0)
    expect(dataset.data_columns).to     eq({"pcpuid"=>{"type"=>"string"}, "the_geom"=>{"type"=>"geometry"}, "cartodb_id"=>{"type"=>"number"}, "the_geom_webmercator"=>{"type"=>"geometry"}})
  end
end
