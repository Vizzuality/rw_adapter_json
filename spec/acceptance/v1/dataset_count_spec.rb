require 'acceptance_helper'

module V1
  describe 'Datasets COUNT', type: :request do
    fixtures :service_settings

    context 'Counters for specific dataset' do
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
                "query": "?returnCountOnly=true&tableName=data&where=iso in ('AUS')",
                "fs": {
                  "tableName": "data",
                  "where": "iso in ('AUS')",
                  "returnCountOnly": true
                }
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
                "query": "?returnCountOnly=true&tableName=data",
                "fs": {
                  "tableName": "data",
                  "returnCountOnly": true
                }
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
                "query": "?returnCountOnly=true&tableName=data&groupByFieldsForStatistics=iso",
                "fs": {
                  "tableName": "data",
                  "groupByFieldsForStatistics": "iso",
                  "returnCountOnly": true
                }
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
                "query": "?returnCountOnly=true&tableName=data&where=iso in ('AUS') and year > '2012'&groupByFieldsForStatistics=year",
                "fs": {
                  "tableName": "data",
                  "groupByFieldsForStatistics": "year",
                  "where": "iso in ('AUS') and year > '2012'",
                  "returnCountOnly": true
                }
              },
              "relationships": {}
            }
          }
        }

        let!(:query_5) {
          {
            "data": {
              "type": "result",
              "id": "undefined",
              "attributes": {
                "query": "?returnCountOnly=true&tableName=data&groupByFieldsForStatistics=iso,year",
                "fs": {
                  "tableName": "data",
                  "groupByFieldsForStatistics": "iso,year",
                  "returnCountOnly": true
                }
              },
              "relationships": {}
            }
          }
        }

        before(:each) do
          stub_request(:get, "http://192.168.99.100:8000/convert/sql2FS?sql=select%20count(*)%20from%20data%20where%20iso%20in%20('AUS')").
          with(:headers => {'Accept'=>'application/json', 'Authentication'=>'3123123der324eewr434ewr4324', 'Content-Type'=>'application/json', 'Expect'=>'', 'User-Agent'=>'Typhoeus - https://github.com/typhoeus/typhoeus'}).
          to_return(:status => 200, :body => Oj.dump(query_1), :headers => {})

          stub_request(:get, "http://192.168.99.100:8000/convert/sql2FS?sql=select%20count(*)%20from%20data").
          with(:headers => {'Accept'=>'application/json', 'Authentication'=>'3123123der324eewr434ewr4324', 'Content-Type'=>'application/json', 'Expect'=>'', 'User-Agent'=>'Typhoeus - https://github.com/typhoeus/typhoeus'}).
          to_return(:status => 200, :body => Oj.dump(query_2), :headers => {})

          stub_request(:get, "http://192.168.99.100:8000/convert/sql2FS?sql=select%20count(*)%20from%20data%20group%20by%20iso").
          with(:headers => {'Accept'=>'application/json', 'Authentication'=>'3123123der324eewr434ewr4324', 'Content-Type'=>'application/json', 'Expect'=>'', 'User-Agent'=>'Typhoeus - https://github.com/typhoeus/typhoeus'}).
          to_return(:status => 200, :body => Oj.dump(query_3), :headers => {})

          stub_request(:get, "http://192.168.99.100:8000/convert/sql2FS?sql=select%20count(*)%20from%20data%20where%20iso%20in%20(%27AUS%27)%20and%20year%20>%20%272012%27%20group%20by%20year").
          with(:headers => {'Accept'=>'application/json', 'Authentication'=>'3123123der324eewr434ewr4324', 'Content-Type'=>'application/json', 'Expect'=>'', 'User-Agent'=>'Typhoeus - https://github.com/typhoeus/typhoeus'}).
          to_return(:status => 200, :body => Oj.dump(query_4), :headers => {})

          stub_request(:get, "http://192.168.99.100:8000/convert/sql2FS?sql=select%20count(year)%20from%20data%20group%20by%20iso,year").
          with(:headers => {'Accept'=>'application/json', 'Authentication'=>'3123123der324eewr434ewr4324', 'Content-Type'=>'application/json', 'Expect'=>'', 'User-Agent'=>'Typhoeus - https://github.com/typhoeus/typhoeus'}).
          to_return(:status => 200, :body => Oj.dump(query_5), :headers => {})
        end

        it 'Select count' do
          post "/query/#{dataset_id}?sql=select count(*) from data", params: params

          data = json['data']

          expect(status).to eq(200)
          expect(data[0]['count']).to eq(5)
        end

        it 'Select count with where' do
          post "/query/#{dataset_id}?sql=select count(*) from data where iso in ('AUS')", params: params

          data = json['data']

          expect(status).to eq(200)
          expect(data[0]['count']).to eq(2)
        end

        it 'Select count with two where' do
          post "/query/#{dataset_id}?sql=select count(*) from data where iso in ('AUS') and year > '2012' group by year", params: params

          data = json['data']

          expect(status).to eq(200)
          expect(data[0]['count']).to eq(1)
        end

        it 'Select count with group by' do
          post "/query/#{dataset_id}?sql=select count(*) from data group by iso", params: params

          data = json['data']

          expect(status).to eq(200)
          expect(data[0]['count']).to eq(3)
          expect(data[1]['count']).to eq(2)
        end

        it 'Select count with group by two attr' do
          post "/query/#{dataset_id}?sql=select count(year) from data group by iso,year", params: params

          data = json['data']

          expect(status).to eq(200)
          # expect(data).to eq(1)
          expect(data[0]['count']).to eq(1)
          expect(data[1]['count']).to eq(3)
          expect(data[2]['count']).to eq(1)
        end
      end
    end
  end
end
