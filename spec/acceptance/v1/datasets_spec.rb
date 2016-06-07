require 'acceptance_helper'

module V1
  describe 'Datasets', type: :request do
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
        it 'Allows access cartoDB data with default limit 1' do
          post "/query/#{dataset_id}", params: params

          data = json['data'][0]

          expect(status).to eq(200)
          expect(data['cartodb_id']).not_to  be_nil
          expect(data['pcpuid']).not_to      be_nil
          expect(data['the_geom']).to        be_present
          expect(json['data_attributes']).to be_present
          expect(json['data'].length).to     eq(1)
        end
      end

      context 'With params' do
        it 'Allows access all available cartoDB data with limit all' do
          post "/query/#{dataset_id}?limit=all", params: params

          data = json['data']

          expect(status).to eq(200)
          expect(data.length).to eq(5)
        end

        it 'Allows access cartoDB data with order ASC' do
          post "/query/#{dataset_id}?order[]=cartodb_id", params: params

          data = json['data'][0]

          expect(status).to eq(200)
          expect(data['cartodb_id']).to  eq('1')
          expect(json['data'].length).to eq(1)
        end

        it 'Allows access cartoDB data with order DESC' do
          post "/query/#{dataset_id}?order[]=-cartodb_id", params: params

          data = json['data'][0]

          expect(status).to eq(200)
          expect(data['cartodb_id']).to eq('5')
        end

        it 'Allows access cartoDB data details with select and order wit data limit 2' do
          post "/query/#{dataset_id}?select[]=cartodb_id,pcpuid&order[]=pcpuid&limit=2", params: params

          data = json['data'][0]

          expect(status).to eq(200)
          expect(data['cartodb_id']).to   eq('5')
          expect(data['pcpuid']).not_to   be_nil
          expect(data['the_geom']).not_to be_present
          expect(json['data'].length).to  eq(2)
        end

        it 'Allows access cartoDB data details with select, filter and order DESC' do
          post "/query/#{dataset_id}?select[]=cartodb_id,pcpuid&filter=(cartodb_id==1,2,4,5 <and> pcpuid><'350558'..'9506590')&order[]=-pcpuid&limit=2", params: params

          data = json['data'][0]

          expect(status).to eq(200)
          expect(data['cartodb_id']).to   eq('1')
          expect(data['pcpuid']).to       eq('500001')
          expect(data['the_geom']).not_to be_present
          expect(json['data'].length).to eq(2)
        end

        it 'Allows access cartoDB data details with select, filter_not and order' do
          post "/query/#{dataset_id}?select[]=cartodb_id,pcpuid&filter_not=(cartodb_id>=4 <and> pcpuid><'500001'..'9506590')&order[]=pcpuid", params: params

          data = json['data'][0]

          expect(status).to eq(200)
          expect(data['cartodb_id']).to   eq('2')
          expect(data['pcpuid']).not_to   be_nil
          expect(data['the_geom']).not_to be_present
        end

        it 'Allows access cartoDB data details without select, all filters and order DESC' do
          post "/query/#{dataset_id}?filter=(cartodb_id==5)&filter_not=(cartodb_id==4 <and> pcpuid><'500001'..'9506590')&order[]=-pcpuid", params: params

          data = json['data'][0]

          expect(status).to eq(200)
          expect(data['cartodb_id']).to eq('5')
          expect(data['pcpuid']).to     be_present
          expect(data['the_geom']).to   be_present
        end

        it 'Allows access cartoDB data details for all filters, order and without select' do
          post "/query/#{dataset_id}?filter=(cartodb_id<<5)&filter_not=(cartodb_id==4 <and> pcpuid><'500001'..'9506590')&order[]=-cartodb_id", params: params

          data = json['data']

          expect(status).to eq(200)
          expect(data.size).to               eq(2)
          expect(data[0]['cartodb_id']).to   eq('3')
          expect(data[0]['pcpuid']).not_to   be_nil
          expect(data[0]['the_geom']).not_to be_nil
          expect(data[1]['cartodb_id']).to   eq('2')
          expect(json['data'].length).to     eq(2)
        end

        it 'Allows access cartoDB data details for all filters without select and order' do
          post "/query/#{dataset_id}?filter=(cartodb_id>=2)&filter_not=(cartodb_id==4 <and> pcpuid><'350659'..'9506590')", params: params

          data = json['data']

          expect(status).to eq(200)
          expect(data[0]['cartodb_id']).to   eq('2')
          expect(data[0]['pcpuid']).not_to   be_nil
          expect(data[0]['the_geom']).not_to be_nil
          expect(data[1]['cartodb_id']).to   eq('5')
        end

        it 'Allows access cartoDB data details for all filters' do
          post "/query/#{dataset_id}?select[]=cartodb_id,pcpuid&filter=(cartodb_id<<5 <and> pcpuid>='350558')&filter_not=(cartodb_id==4 <and> pcpuid><'350640'..'9506590')&order[]=-pcpuid", params: params

          data = json['data']

          expect(status).to eq(200)
          expect(data.size).to             eq(1)
          expect(data[0]['cartodb_id']).to eq('2')
          expect(data[0]['pcpuid']).not_to be_nil
          expect(data[0]['the_geom']).to   be_nil
        end
      end
    end
  end
end
