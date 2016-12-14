require 'acceptance_helper'

module V1
  describe 'Datasets Meta', type: :request do
    context 'Create and delete dataset' do
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
                      "data_id": "fd2a6bab-5697-404b-9cf9-5905bba17712",
                      "start_date": Time.now,
                      "end_date": Time.now + 1.days,
                      "special_string": "L'Este"
                    },
                    {
                      "pcpuid": "350659",
                      "the_geom": "0101000020E6100000000000000C671541000000000C671541",
                      "cartodb_id": 3,
                      "data_id": "fd2a6bab-5697-404b-9cf9-5905bba17713",
                      "start_date": Time.now,
                      "end_date": Time.now + 1.days,
                      "special_string": "L'Este"
                    },
                    {
                      "pcpuid": "481347",
                      "the_geom": "0101000020E6100000000000000C611D41000000000C611D41",
                      "cartodb_id": 4,
                      "data_id": "fd2a6bab-5697-404b-9cf9-5905bba17714",
                      "start_date": Time.now,
                      "end_date": Time.now + 1.days,
                      "special_string": "L'Este"
                    },
                    {
                      "pcpuid": "120171",
                      "the_geom": "0101000020E610000000000000B056FD4000000000B056FD40",
                      "cartodb_id": 5,
                      "data_id": "fd2a6bab-5697-404b-9cf9-5905bba17715",
                      "start_date": Time.now,
                      "end_date": Time.now + 1.days,
                      "special_string": "L'Este"
                    },
                    {
                      "pcpuid": "500001",
                      "the_geom": "0101000020E610000000000000806EF84000000000806EF840",
                      "cartodb_id": 1,
                      "data_id": data_id,
                      "start_date": Time.now,
                      "end_date": Time.now + 1.days,
                      "special_string": "L'Este"
                  }]}

      let!(:data_without_path) {
        [{"count": 1, "confidence": "confirmed", "country_iso": "BRA", "state_iso": "BRA1", "year": "2016", "day": 9}, {"count": 13, "confidence": "confirmed", "country_iso": "BRA", "state_iso": "BRA1", "year": "2016", "day": 10}]
      }

      let!(:params) {{"connector": {
                      "data_columns": Oj.dump(data_columns),
                      "data": Oj.dump(data),
                      "legend": {"long": "123", "lat": "123", "country": "pais", "region": "barrio", "date": ["start_date", "end_date"]}
                    }}}

      let!(:external_params) {{"connector": {"id": "fd2a6bab-5697-404b-9cf9-5905bba17751",
                                             "connector_url": "http://192.168.99.100:8000/query/5306fd54-df71-4e20-8b34-2ff464ab28be"
                             }}}

      let!(:dataset) {
        dataset = Dataset.create!(data: data, data_columns: data_columns)
        dataset
      }

      let!(:dataset_id) { dataset.id }


      it 'Allows to create json dataset with data and fields' do
        post '/datasets', params: params

        expect(status).to eq(201)
        expect(json_main['message']).to eq('Dataset created')
        expect(Dataset.last.reload.data[0]['start_date']).to eq(Time.now.to_datetime.iso8601)
        expect(Dataset.last.reload.data[0]['end_date']).to   eq((Time.now + 1.days).to_datetime.iso8601)
      end

      context 'Create JSON dataset from external json' do
        before(:each) do
          stub_request(:get, 'http://192.168.99.100:8000/query/5306fd54-df71-4e20-8b34-2ff464ab28be').
          with(:headers => {'Accept' => 'application/json', 'Content-Type' => 'application/json'}).
          to_return(status: 200, body: Oj.dump(data), headers: {})
        end

        it 'Allows to create json dataset' do
          post '/datasets', params: external_params

          expect(status).to eq(201)
          expect(json_main['message']).to                  eq('Dataset created')
          expect(Dataset.find(dataset_id).data_columns).to eq({"pcpuid"=>{"type"=>"string"}, "the_geom"=>{"type"=>"geometry"}, "cartodb_id"=>{"type"=>"number"}, "the_geom_webmercator"=>{"type"=>"geometry"}})
          expect(Dataset.find(dataset_id).data).not_to     be_empty
        end
      end

      context 'Update with external url' do
        before(:each) do
          stub_request(:get, 'http://192.168.99.100:8000/query/5306fd54-df71-4e20-8b34-2ff464ab28be').
          to_return(status: 200, body: Oj.dump(data), headers: {})

          stub_request(:get, 'http://gfw2-data.s3.amazonaws.com/climate/glad_country_pages.json').
          to_return(status: 200, body: Oj.dump(data_without_path), headers: {})
        end

        it 'Allows to update dataset' do
          post "/datasets/#{dataset_id}", params: {"connector": {"id": "#{dataset_id}",
                                                   "connector_url": "http://192.168.99.100:8000/query/5306fd54-df71-4e20-8b34-2ff464ab28be"
                                                  }}

          expect(status).to eq(200)
          expect(json_main['message']).to                      eq('Dataset updated')
          expect(Dataset.find(dataset_id).data_columns).not_to be_empty
          expect(Dataset.find(dataset_id).data).not_to         be_empty
        end

        it 'Allows to update dataset without data_path' do
          post "/datasets/#{dataset_id}", params: {"connector": {"id": "#{dataset_id}",
                                                   "connector_url": "http://gfw2-data.s3.amazonaws.com/climate/glad_country_pages.json"
                                                  }}

          expect(status).to eq(200)
          expect(json_main['message']).to                  eq('Dataset updated')
          expect(Dataset.find(dataset_id).data_columns).to eq({"pcpuid"=>{"type"=>"string"}, "the_geom"=>{"type"=>"geometry"}, "cartodb_id"=>{"type"=>"number"}, "the_geom_webmercator"=>{"type"=>"geometry"}})
          expect(Dataset.find(dataset_id).data).not_to     be_empty
        end

        it 'Allows to update dataset with data_path root_path' do
          post "/datasets/#{dataset_id}", params: {"connector": {"id": "#{dataset_id}",
                                                   "connector_url": "http://gfw2-data.s3.amazonaws.com/climate/glad_country_pages.json",
                                                   "data_path": "root_path"
                                                  }}

          expect(status).to eq(200)
          expect(json_main['message']).to                      eq('Dataset updated')
          expect(Dataset.find(dataset_id).data_columns).not_to be_empty
          expect(Dataset.find(dataset_id).data).not_to         be_empty
        end
      end

      it 'Allows to overwrite dataset data with empty object' do
        post "/datasets/#{dataset_id}/overwrite", params: {"connector": {"id": "#{dataset_id}",
                                                                "data": ""
                                                               }}

        expect(status).to eq(200)
        expect(json_main['message']).to                  eq('Dataset data replaced')
        expect(Dataset.find(dataset_id).data_columns).to be_nil
        expect(Dataset.find(dataset_id).data).to         eq([])
      end

      it 'Allows to overwrite dataset data' do
        post "/datasets/#{dataset_id}/overwrite", params: {"connector": {"id": "#{dataset_id}",
                                                                "data": Oj.dump([{ "pcpuid": "900001" }])
                                                               }}

        expect(status).to eq(200)
        expect(json_main['message']).to                  eq('Dataset data replaced')
        expect(Dataset.find(dataset_id).data.size).to    eq(1)
        expect(Dataset.find(dataset_id).reload.data_columns).to eq({"pcpuid"=>{"type"=>"string"}, "data_id"=>{"type"=>"string"}})
      end

      it 'Allows to update dataset data' do
        post "/datasets/#{dataset_id}/data/#{data_id}", params: {"connector": {"id": "#{dataset_id}",
                                                                 "data_id": "#{data_id}",
                                                                 "data": Oj.dump({ "pcpuid": "900001" })
                                                                }}

        expect(status).to eq(200)
        expect(json_main['message']).to                      eq('Dataset updated')
        expect(Dataset.find(dataset_id).data_columns).not_to be_empty
        expect(Dataset.find(dataset_id).data.find_all { |d| d['data_id'] == "#{data_id}" }.to_s).to include('900001')
      end

      it 'Allows to delete dataset data' do
        delete "/datasets/#{dataset_id}/data/#{data_id}"

        expect(status).to eq(200)
        expect(json_main['message']).to                      eq('Dataset data deleted')
        expect(Dataset.find(dataset_id).data_columns).not_to be_empty
        expect(Dataset.find(dataset_id).data.find_all { |d| d['data_id'] == "#{data_id}" }.to_s).not_to include('500001')
      end

      it 'Allows to update dataset with data' do
        post "/datasets/#{dataset_id}", params: {"connector": {"id": "#{dataset_id}",
                                                 "data": Oj.dump(data)
                                                }}

        expect(status).to eq(200)
        expect(json_main['message']).to                      eq('Dataset updated')
        expect(Dataset.find(dataset_id).data_columns).not_to be_empty
        expect(Dataset.find(dataset_id).data).not_to         be_empty
      end

      it 'Allows to delete dataset' do
        delete "/datasets/#{dataset_id}"

        expect(status).to eq(200)
        expect(json_main['message']).to eq('Dataset deleted')
        expect(Dataset.where(id: dataset_id)).to be_empty
      end
    end
  end
end
