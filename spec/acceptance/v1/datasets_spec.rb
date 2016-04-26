require 'acceptance_helper'

module V1
  describe 'Datasets', type: :request do
    fixtures :datasets

    context 'For specific dataset' do
      let!(:params) {{"dataset": {
                      "connector_type": "json",
                      "id": "#{dataset_id}",
                      "connector_provider": "RwJson",
                      "dataset_attributes": {"name": "Json test api", "format": 0, "data_path": "rows", "attributes_path": "fields"},
                      "data_attributes": {
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

      let!(:dataset_id) { Dataset.first.id }

      context 'Without params' do
        it 'Allows access cartoDB data' do
          # raw_response_file = File.new('spec/support/response/#{dataset_id}.json').read
          # stub_request(:any, /rschumann.cartodb.com/).to_return(body: raw_response_file)
          post "/query/#{dataset_id}", params: params

          data = json['data'][0]

          expect(status).to eq(200)
          expect(data['cartodb_id']).not_to be_nil
          expect(data['pcpuid']).not_to     be_nil
          expect(data['the_geom']).to       be_present
        end
      end

      context 'With params' do
        it 'Allows access cartoDB data with order ASC' do
          post "/query/#{dataset_id}?order[]=cartodb_id", params: params

          data = json['data'][0]

          expect(status).to eq(200)
          expect(data['cartodb_id']).to eq('1')
        end

        it 'Allows access cartoDB data with order DESC' do
          post "/query/#{dataset_id}?order[]=-cartodb_id", params: params

          data = json['data'][0]

          expect(status).to eq(200)
          expect(data['cartodb_id']).to eq('5')
        end

        it 'Allows access cartoDB data details with select and order' do
          post "/query/#{dataset_id}?select[]=cartodb_id,pcpuid&order[]=pcpuid", params: params

          data = json['data'][0]

          expect(status).to eq(200)
          expect(data['cartodb_id']).to   eq('5')
          expect(data['pcpuid']).not_to   be_nil
          expect(data['the_geom']).not_to be_present
        end

        it 'Allows access cartoDB data details with select, filter and order DESC' do
          post "/query/#{dataset_id}?select[]=cartodb_id,pcpuid&filter=(cartodb_id==1,2,4,5 <and> pcpuid><'350558'..'9506590')&order[]=-pcpuid", params: params

          data = json['data'][0]

          expect(status).to eq(200)
          expect(data['cartodb_id']).to   eq('1')
          expect(data['pcpuid']).to       eq('500001')
          expect(data['the_geom']).not_to be_present
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
