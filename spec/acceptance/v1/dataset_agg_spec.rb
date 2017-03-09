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
                              "type": "number"
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
                      "year": '2011',
                      "population": "2500",
                      "loss": "1000"
                    },
                    {
                      "iso": "AUS",
                      "year": '2013',
                      "population": "500",
                      "loss": "2000"
                    },
                    {
                      "iso": "ESP",
                      "year": '2014',
                      "population": "500",
                      "loss": "3000"
                    },
                    {
                      "iso": "ESP",
                      "year": '2014',
                      "population": "500",
                      "loss": "4000"
                    },
                    {
                      "iso": "ESP",
                      "year": '2014',
                      "population": "500",
                      "loss": "5000"
                  }]}

      let!(:dataset) {
        dataset = Dataset.create!(data_columns: data_columns)
        dataset.data_values.create(data: data[0])
        dataset.data_values.create(data: data[1])
        dataset.data_values.create(data: data[2])
        dataset.data_values.create(data: data[3])
        dataset.data_values.create(data: data[4])
        dataset
      }

      let!(:dataset_id) { dataset.id }

      let!(:params) {{"connector":{"dataset": {"data": {
                                  "id": "#{dataset_id}",
                                  "attributes": {"name": "Json test api new",
                                                                    "data_path": "data",
                                                                    "attributes_path": "fields",
                                                                    "provider": "RwJson",
                                                                    "format": "JSON",
                                                                    "meta": {
                                                                      "status": "saved",
                                                                      "updated_at": "2016-04-29T09:58:20.048Z",
                                                                      "created_at": "2016-04-29T09:58:19.739Z"
                                                                    }}
                                }}}}}

      let(:group_attr_1) { URI.encode(Oj.dump([{"onStatisticField":"population","statisticType":"sum","outStatisticFieldName":"population"},{"onStatisticField":"loss","statisticType":"avg","outStatisticFieldName":"loss"}])) }

      context 'Aggregation with params' do
        let!(:query_1) {
          {
            "data": {
              "type": "result",
              "id": "undefined",
              "attributes": {
                "query": "?outFields=iso,year&outStatistics=[{\"onStatisticField\":\"population\",\"statisticType\":\"sum\",\"outStatisticFieldName\":\"population\"},{\"onStatisticField\":\"loss\",\"statisticType\":\"avg\",\"outStatisticFieldName\":\"loss\"}]&tableName=data&where=iso in ('AUS','ESP')&groupByFieldsForStatistics=iso,year&orderByFields=iso"
              },
              "relationships": {}
            }
          }
        }

        let!(:query_2) {
          {
            "data": {
              "type": "result",
              "id": "undefined",
              "attributes": {
                "query": "?outFields=iso&outStatistics=[{\"onStatisticField\":\"population\",\"statisticType\":\"max\"}]&tableName=data&where=iso in ('ESP','AUS')&groupByFieldsForStatistics=iso&orderByFields=iso"
              },
              "relationships": {}
            }
          }
        }

        let!(:query_3) {
          {
            "data": {
              "type": "result",
              "id": "undefined",
              "attributes": {
                "query": "?outFields=year&outStatistics=[{\"onStatisticField\":\"population\",\"statisticType\":\"sum\",\"outStatisticFieldName\":\"population\"}]&tableName=data&groupByFieldsForStatistics=year&orderByFields=year ASC"
              },
              "relationships": {}
            }
          }
        }

        let!(:query_4) {
          {
            "data": {
              "type": "result",
              "id": "undefined",
              "attributes": {
                "query": "?outFields=years&outStatistics=[{\"onStatisticField\":\"population\",\"statisticType\":\"sum\"}]&tableName=data&groupByFieldsForStatistics=year&orderByFields=year ASC"
              },
              "relationships": {}
            }
          }
        }

        before(:each) do
          stub_request(:get, "http://192.168.99.100:8000/convert/sql2FS?sql=select%20iso,sum(population)%20as%20population,year,avg(loss)%20as%20loss%20from%20data%20where%20iso%20in%20('AUS','ESP')%20group%20by%20iso,year%20order%20by%20iso").
          with(:headers => {'Accept'=>'application/json', 'Authentication'=>'not_a_token', 'Content-Type'=>'application/json', 'Expect'=>'', 'User-Agent'=>'Typhoeus - https://github.com/typhoeus/typhoeus'}).
          to_return(:status => 200, :body => Oj.dump(query_1), :headers => {})
          
          stub_request(:get, "http://192.168.99.100:8000/convert/sql2FS?sql=select%20iso,sum(population)%20as%20population,year,avg(loss)%20as%20loss%20from%20data%20where%20iso%20in%20('AUS','ESP')%20group%20by%20iso,year%20order%20by%20iso").
          with(:headers => {'Accept'=>'application/json', 'Authentication'=>'3123123der324eewr434ewr4324', 'Content-Type'=>'application/json', 'Expect'=>'', 'User-Agent'=>'Typhoeus - https://github.com/typhoeus/typhoeus'}).
          to_return(:status => 200, :body => Oj.dump(query_1), :headers => {})

          stub_request(:get, "http://192.168.99.100:8000/convert/sql2FS?sql=select%20iso,max(population)%20from%20data%20where%20iso%20in%20('ESP','AUS')%20group%20by%20iso%20order%20by%20iso").
          with(:headers => {'Accept'=>'application/json', 'Authentication'=>'3123123der324eewr434ewr4324', 'Content-Type'=>'application/json', 'Expect'=>'', 'User-Agent'=>'Typhoeus - https://github.com/typhoeus/typhoeus'}).
          to_return(:status => 200, :body => Oj.dump(query_2), :headers => {})

          stub_request(:get, "http://192.168.99.100:8000/convert/sql2FS?sql=select%20year,sum(population)%20as%20population%20from%20data%20group%20by%20year%20order%20by%20year%20ASC").
          with(:headers => {'Accept'=>'application/json', 'Authentication'=>'3123123der324eewr434ewr4324', 'Content-Type'=>'application/json', 'Expect'=>'', 'User-Agent'=>'Typhoeus - https://github.com/typhoeus/typhoeus'}).
          to_return(:status => 200, :body => Oj.dump(query_3), :headers => {})

          stub_request(:get, "http://192.168.99.100:8000/convert/sql2FS?sql=select%20years,sum(population)%20from%20data%20group%20by%20year%20order%20by%20year%20ASC").
          with(:headers => {'Accept'=>'application/json', 'Authentication'=>'3123123der324eewr434ewr4324', 'Content-Type'=>'application/json', 'Expect'=>'', 'User-Agent'=>'Typhoeus - https://github.com/typhoeus/typhoeus'}).
          to_return(:status => 200, :body => Oj.dump(query_4), :headers => {})
        end

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
          expect(data[0]['population']).to eq(2500)
          expect(data[1]['iso']).to eq('ESP')
          expect(data[1]['population']).to eq(500)
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
          expect(data['error'][0]).to match('')
        end
      end
    end
  end
end
