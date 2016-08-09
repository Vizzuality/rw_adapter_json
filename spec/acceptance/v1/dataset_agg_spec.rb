require 'acceptance_helper'

module V1
  describe 'Datasets AGG', type: :request do
    fixtures :service_settings

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
                      "population": "2500",
                      "loss": "1000"
                    },
                    {
                      "iso": "AUS",
                      "year": "2013",
                      "population": "500",
                      "loss": "2000"
                    },
                    {
                      "iso": "ESP",
                      "year": "2014",
                      "population": "500",
                      "loss": "3000"
                    },
                    {
                      "iso": "ESP",
                      "year": "2014",
                      "population": "500",
                      "loss": "4000"
                    },
                    {
                      "iso": "ESP",
                      "year": "2014",
                      "population": "500",
                      "loss": "5000"
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

      let(:group_attr_1) { URI.encode(Oj.dump([{"onStatisticField":"population","statisticType":"sum","outStatisticFieldName":"population"},{"onStatisticField":"loss","statisticType":"avg","outStatisticFieldName":"loss"}])) }

      context 'Aggregation with params' do
        it 'Allows aggregate JSON data by one sum attribute and group by two attributes using FS' do
          post "/query/#{dataset_id}?outFields=iso,population,year&outStatistics=#{group_attr_1}&tableName=data&where=iso in ('AUS','ESP')&groupByFieldsForStatistics=iso,year&orderByFields=iso", params: params

          data = json['data']

          expect(status).to eq(200)
          expect(data.length).to           eq(3)
          expect(data[0]['population']).to eq(2500)
          expect(data[0]['loss']).to       eq(1000.0)
          expect(data[0]['year']).to       eq('2011')
          expect(data[1]['population']).to eq(500)
          expect(data[1]['loss']).to       eq(2000.0)
          expect(data[1]['year']).to       eq('2013')
          expect(data[2]['population']).to eq(1500) # 3x500
          expect(data[2]['loss']).to       eq(4000.0)
          expect(data[2]['year']).to       eq('2014')
        end

        it 'Allows aggregate JSON data by one sum attribute and group by two attributes using SQL' do
          post "/query/#{dataset_id}?sql=select iso,sum(population) as population,year,avg(loss) as loss from data where iso in ('AUS','ESP') group by iso,year order by iso", params: params

          data = json['data']

          expect(status).to eq(200)
          expect(data.length).to           eq(3)
          expect(data[0]['population']).to eq(2500)
          expect(data[0]['loss']).to       eq(1000.0)
          expect(data[0]['year']).to       eq('2011')
          expect(data[1]['population']).to eq(500)
          expect(data[1]['loss']).to       eq(2000.0)
          expect(data[1]['year']).to       eq('2013')
          expect(data[2]['population']).to eq(1500) # 3x500
          expect(data[2]['loss']).to       eq(4000.0)
          expect(data[2]['year']).to       eq('2014')
        end

        it 'Allows aggregate JSON data by one max attribute and group by one attribute using SQL' do
          post "/query/#{dataset_id}?sql=select iso,max(population) from data where iso in ('ESP','AUS') group by iso order by iso", params: params

          data = json['data']

          expect(status).to eq(200)
          expect(data.length).to    eq(2)
          expect(data[0]['iso']).to eq('AUS')
          expect(data[0]['max']).to eq(2500)
          expect(data[1]['iso']).to eq('ESP')
          expect(data[1]['max']).to eq(500)
        end

        it 'Allows aggregate JSON data by one sum attribute and group by one attribute using SQL' do
          post "/query/#{dataset_id}?sql=select year,sum(population) as population from data group by year order by year ASC", params: params

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
          post "/query/#{dataset_id}?sql=select years,sum(population) from data group by year order by year ASC", params: params

          data = json['data']

          expect(status).to eq(200)
          expect(data['error'][0]).to match("ERROR: column \"years\" does not exist")
        end
      end
    end
  end
end
