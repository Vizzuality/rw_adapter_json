require 'acceptance_helper'

module V1
  describe 'Datasets', type: :request do
    fixtures :service_settings

    context 'For specific dataset' do
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

      let!(:dataset) {
        dataset = Dataset.create!(data: data, data_columns: data_columns)
        dataset
      }

      let!(:dataset_id) { Dataset.first.id }

      let!(:params) {{"dataset": {
                      "id": "#{dataset_id}",
                      "name": "Json test api",
                      "data_path": "data",
                      "attributes_path": "fields",
                      "provider": "RwJson",
                      "format": "JSON",
                      "meta": {
                        "status": "saved",
                        "updated_at": "2016-04-29T09:58:20.048Z",
                        "created_at": "2016-04-29T09:58:19.739Z"
                      }
                    }}}

      context 'Without params' do
        it 'Allows access Json data with default limit 1' do
          post "/query/#{dataset_id}", params: params

          data = json['data'][0]

          expect(status).to eq(200)
          expect(data['cartodb_id']).not_to be_nil
          expect(data['pcpuid']).not_to     be_nil
          expect(data['the_geom']).to       be_present
          expect(json['fields']).to         be_present
          expect(json['data'].length).to    eq(1)
        end
      end

      context 'With params' do
        it 'Allows access all available Json data with limit all' do
          post "/query/#{dataset_id}?limit=all", params: params

          data = json['data']

          expect(status).to eq(200)
          expect(data.length).to eq(5)
        end

        it 'Allows access Json data with order ASC' do
          post "/query/#{dataset_id}?orderByFields=cartodb_id ASC&limit=1", params: params

          data = json['data'][0]

          expect(status).to eq(200)
          expect(data['cartodb_id']).to  eq('1')
          expect(json['data'].length).to eq(1)
        end

        it 'Allows access Json data with order DESC using FS' do
          post "/query/#{dataset_id}?orderByFields=cartodb_id DESC", params: params

          data = json['data'][0]

          expect(status).to eq(200)
          expect(data['cartodb_id']).to eq('5')
        end

        it 'Allows access Json data details with select and order wit data limit 2 using FS' do
          post "/query/#{dataset_id}?outFields=cartodb_id,pcpuid&orderByFields=pcpuid ASC&limit=2", params: params

          data = json['data'][0]

          expect(status).to eq(200)
          expect(data['cartodb_id']).to   eq('5')
          expect(data['pcpuid']).not_to   be_nil
          expect(data['the_geom']).not_to be_present
          expect(json['data'].length).to  eq(2)
        end

        it 'Allows access Json data details with select, filter and order DESC using SQL' do
          post "/query/#{dataset_id}?sql=select cartodb_id,pcpuid from data where cartodb_id in ('1','2','4','5') and pcpuid between '350558' and '9506590' order by pcpuid DESC", params: params

          data = json['data'][0]

          expect(status).to eq(200)
          expect(data['cartodb_id']).to   eq('1')
          expect(data['pcpuid']).to       eq('500001')
          expect(data['the_geom']).not_to be_present
          expect(json['data'].length).to eq(3)
        end

        it 'Allows access Json data details with select, filter_not and order' do
          post "/query/#{dataset_id}?sql=select cartodb_id,pcpuid from data where cartodb_id >= '4' and pcpuid between '200001' and '9506590' order by pcpuid ASC", params: params

          data = json['data'][0]

          expect(status).to eq(200)
          expect(data['cartodb_id']).to   eq('4')
          expect(data['pcpuid']).not_to   be_nil
          expect(data['the_geom']).not_to be_present
        end

        it 'Allows access Json data details without select, all filters and order DESC' do
          post "/query/#{dataset_id}?where=cartodb_id = '5' and cartodb_id != '4' and pcpuid not between '500001' and '9506590'&orderByFields=pcpuid DESC", params: params

          data = json['data'][0]

          expect(status).to eq(200)
          expect(data['cartodb_id']).to eq('5')
          expect(data['pcpuid']).to     be_present
          expect(data['the_geom']).to   be_present
        end

        it 'Allows access Json data details for all filters, order and without select' do
          post "/query/#{dataset_id}?where=cartodb_id < '5' and cartodb_id != '4' and pcpuid between '500001' and '9506590'&orderByFields=cartodb_id DESC", params: params

          data = json['data']

          expect(status).to eq(200)
          expect(data.size).to               eq(1)
          expect(data[0]['cartodb_id']).to   eq('1')
          expect(data[0]['pcpuid']).not_to   be_nil
          expect(data[0]['the_geom']).not_to be_nil
        end

        it 'Allows access Json data details for all filters without select and order' do
          post "/query/#{dataset_id}?sql=select * from data where cartodb_id >= '2' and cartodb_id != '4' and pcpuid between '350659' and '9506590'", params: params

          data = json['data']

          expect(status).to eq(200)
          expect(data[0]['cartodb_id']).to   eq('3')
        end

        it 'Allows access Json data details for all filters' do
          post "/query/#{dataset_id}?sql=select cartodb_id,pcpuid from data where cartodb_id < '5' and pcpuid >= '350558' and cartodb_id != '4' and pcpuid not between '350640' and '450590' order by pcpuid DESC", params: params

          data = json['data']

          expect(status).to eq(200)
          expect(data.size).to             eq(2)
          expect(data[0]['cartodb_id']).to eq('1')
          expect(data[0]['pcpuid']).not_to be_nil
          expect(data[0]['the_geom']).to   be_nil
        end
      end
    end
  end
end
