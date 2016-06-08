require 'acceptance_helper'

module V1
  describe 'Datasets AGG', type: :request do
    context 'Aggregation for specific dataset' do
      let!(:data_columns) {{
                            "iso": {
                              "type": "string"
                            },
                            "name": {
                              "type": "string"
                            },
                            "year": {
                              "type": "string"
                            },
                            "the_geom": {
                              "type": "geometry"
                            },
                            "cartodb_id": {
                              "type": "number"
                            },
                            "population": {
                              "type": "number"
                            },
                            "the_geom_webmercator": {
                              "type": "geometry"
                            }
                          }}

      let!(:data) {[{
                      "iso": "AUS",
                      "year": "2011",
                      "population": "2500"
                    },
                    {
                      "iso": "AUS",
                      "year": "2013",
                      "population": "500"
                    },
                    {
                      "iso": "ESP",
                      "year": "2014",
                      "population": "500"
                    },
                    {
                      "iso": "ESP",
                      "year": "2014",
                      "population": "500"
                    },
                    {
                      "iso": "ESP",
                      "year": "2014",
                      "population": "500"
                  }]}

      let!(:dataset) {
        dataset = Dataset.create!(data: data, data_columns: data_columns)
        dataset
      }

      let!(:dataset_id) { dataset.id }

      let!(:params) {{"dataset": {
                      "id": "#{dataset_id}",
                      "name": "Json test api new",
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

      context 'Aggregation with params' do
        it 'Allows aggregate JSON data by one sum attribute and group by two attributes' do
          post "/query/#{dataset_id}?select[]=iso,population,year&filter=(iso=='AUS','ESP')&aggr_by[]=population&aggr_func=sum&group_by=iso,year&order[]=iso", params: params

          data = json['data']

          expect(status).to eq(200)
          expect(data.length).to             eq(3)
          expect(json['data_attributes']).to be_present
          expect(data[0]['population']).to   eq(2500)
          expect(data[0]['year']).to         eq('2011')
          expect(data[1]['population']).to   eq(500)
          expect(data[1]['year']).to         eq('2013')
          expect(data[2]['population']).to   eq(1500) # 3x500
        end

        it 'Allows aggregate JSON data by one max attribute and group by one attribute' do
          post "/query/#{dataset_id}?select[]=iso,population&filter=(iso=='ESP','AUS')&aggr_by[]=population&aggr_func=max&group_by=iso&order[]=iso", params: params

          data = json['data']

          expect(status).to eq(200)
          expect(data.length).to           eq(2)
          expect(data[0]['iso']).to        eq('AUS')
          expect(data[0]['population']).to eq(2500)
          expect(data[1]['iso']).to        eq('ESP')
          expect(data[1]['population']).to eq(500)
        end

        it 'Allows aggregate JSON data by one sum attribute and group by one attribute' do
          post "/query/#{dataset_id}?select[]=year,population&aggr_by[]=population&aggr_func=sum&group_by=year&order[]=year", params: params

          data = json['data']

          expect(status).to eq(200)
          expect(data.length).to           eq(3)
          expect(data[0]['population']).to eq(2500)
          expect(data[0]['year']).to       eq('2011')
          expect(data[1]['population']).to eq(500)
          expect(data[1]['year']).to       eq('2013')
          expect(data[2]['population']).to eq(1500)
        end

        it 'Return error message for wrong params' do
          post "/query/#{dataset_id}?select[]=iso,population&filter=(isoss=='ESP','AUS')&aggr_by[]=population&group_by=isoss&aggr_func=max&order[]=isoss", params: params

          data = json['data']

          expect(status).to eq(200)
          expect(data['error'][0]).to match("ERROR: column \"isoss\" does not exist")
        end
      end
    end
  end
end
