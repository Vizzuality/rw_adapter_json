require 'acceptance_helper'

module V1
  describe 'Datasets Meta', type: :request do
    context 'Create and delete dataset' do
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
                  }]}

      let!(:params) {{"connector": {
                      "data_columns": Oj.dump(data_columns),
                      "data": Oj.dump(data)
                    }}}

      let!(:dataset) {
        dataset = Dataset.create!(data: data, data_columns: data_columns)
        dataset
      }

      let!(:dataset_id) { Dataset.first.id }

      it 'Allows to create json dataset with data and data_attributes' do
        post '/datasets', params: params

        expect(status).to eq(201)
        expect(json['message']).to eq('Dataset created')
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
