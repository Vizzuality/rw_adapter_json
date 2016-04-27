require 'acceptance_helper'

module V1
  describe 'Datasets Meta', type: :request do
    context 'Create and delete dataset' do
      fixtures :datasets

      let!(:dataset_id) { Dataset.first.id }

      let!(:params) {{"connector": {
                      "id": "2ba3c83c-895d-419e-8f0f-5fbabb7c9f7b",
                      "data_columns": {
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
                      },
                      "data": [{
                        "pcpuid": "350558",
                        "the_geom": "0101000020E610000000000000786515410000000078651541",
                        "cartodb_id": 2
                      },
                      {
                        "pcpuid": "350659",
                        "the_geom": "0101000020E6100000000000000C671541000000000C671541",
                        "cartodb_id": 3
                      },
                      {
                        "pcpuid": "481347",
                        "the_geom": "0101000020E6100000000000000C611D41000000000C611D41",
                        "cartodb_id": 4
                      },
                      {
                        "pcpuid": "120171",
                        "the_geom": "0101000020E610000000000000B056FD4000000000B056FD40",
                        "cartodb_id": 5
                      },
                      {
                        "pcpuid": "500001",
                        "the_geom": "0101000020E610000000000000806EF84000000000806EF840",
                        "cartodb_id": 1
                      }]
                    }}}

      let!(:params_failed) {{"connector": {
                             "id": "2ba3c83c-895d-419e-8f0f-5fbabb7c9f7b",
                             "data_columns": {
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
                             }
                           }}}

      it 'Allows to create json dataset with data and data_attributes' do
        post '/datasets', params: params

        expect(status).to eq(201)
        expect(json['message']).to eq('Dataset created')
      end

      it 'If data failed' do
        post "/datasets", params: params_failed

        expect(status).to eq(422)
        expect(json['success']).to eq(false)
        expect(Dataset.where(id: '2ba3c83c-895d-419e-8f0f-5fbabb7c9f7b')).to be_empty
      end

      it 'Allows to delete dataset' do
        delete "/datasets/#{dataset_id}"

        expect(status).to eq(200)
        expect(json['message']).to eq('Dataset deleted')
        expect(Dataset.where(id: dataset_id)).to be_empty
      end
    end
  end
end
