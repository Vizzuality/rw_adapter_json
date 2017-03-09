# frozen_string_literal: true
module V1
  class ConnectorsController < ApplicationController
    before_action :set_connector,    except: :info
    before_action :set_query_filter, except: :info
    before_action :set_uri,          except: :info
    before_action :set_dataset,      only:  [:show, :update, :update_data, :overwrite, :destroy, :delete_data]
    before_action :overwritable,     only:  [:update, :update_data, :overwrite, :delete_data]
    after_action  :start_gc,         only:   :show

    include Authorization

    def show
      @data = DataStream.new(@connector.data(@query_filter))
      render json: @connector, serializer: ConnectorSerializer, root: false, uri: @uri, data: @data
    end

    def create
      begin
        @dataset = JsonConnector.build_dataset(connector_params)
        @dataset.save
        success_notifier('saved', 'Dataset created', 201)
      rescue
        fail_notifier(nil, 'Error creating dataset')
      end
    end

    def update
      begin
        JsonConnector.update_dataset(connector_params)
        success_notifier('saved', 'Dataset updated', 200)
      rescue
        fail_notifier(nil, 'Error updating dataset')
      end
    end

    def update_data
      begin
        JsonConnector.update_data_object(connector_params)
        success_notifier('saved', 'Dataset updated', 200)
      rescue
        fail_notifier(nil, 'Error updating dataset')
      end
    end

    def overwrite
      begin
        JsonConnector.overwrite_data(connector_params)
        success_notifier('saved', 'Dataset data replaced', 200)
      rescue
        fail_notifier(nil, 'Error replacing dataset')
      end
    end

    def delete_data
      begin
        JsonConnector.delete_data_object(params)
        success_notifier('saved', 'Dataset data deleted', 200)
      rescue
        fail_notifier(nil, 'Error deleting dataset data')
      end
    end

    def destroy
      @dataset.destroy
      begin
        Dataset.notifier(params[:id], 'deleted') if ENV["GATEWAY_TOKEN"].present?
        render json: { message: 'Dataset deleted' }, status: 200
      rescue ActiveRecord::RecordNotDestroyed
        return render json: @dataset.erors, message: 'Dataset could not be deleted', status: 422
      end
    end

    def fields
      render json: @connector, serializer: ConnectorFieldsSerializer, root: false
    end

    private

      def set_connector
        @connector = JsonConnector.new(params) if params[:dataset].present? || params[:connector].present?
      end

      def set_dataset
        @dataset = Dataset.select(:id).find(params[:id])
      end

      def set_query_filter
        # For convert endpoint fs2SQL
        @query_filter = {}
        @query_filter['limit']                      = params[:limit]                      if params[:limit].present?
        @query_filter['outFields']                  = params[:outFields]                  if params[:outFields].present?
        @query_filter['orderByFields']              = params[:orderByFields]              if params[:orderByFields].present?
        @query_filter['resultRecordCount']          = params[:resultRecordCount]          if params[:resultRecordCount].present?
        @query_filter['where']                      = params[:where]                      if params[:where].present?
        @query_filter['tableName']                  = params[:tableName]                  if params[:tableName].present?
        @query_filter['groupByFieldsForStatistics'] = params[:groupByFieldsForStatistics] if params[:groupByFieldsForStatistics].present?
        @query_filter['outStatistics']              = params[:outStatistics]              if params[:outStatistics].present?
        @query_filter['statisticType']              = params[:statisticType]              if params[:statisticType].present?
        # For convert endpoint sql2SQL
        @query_filter['sql']                        = params[:sql]                        if params[:sql].present?
      end

      def set_uri
        @uri = {}
        @uri['api_gateway_url'] = Service::SERVICE_URL
        @uri['full_path']       = request.fullpath
        @uri
      end

      def notify(dataset_id, status=nil)
        Dataset.notifier(dataset_id, status) if ENV["GATEWAY_TOKEN"].present?
      end

      def connector_params
        if params[:data].present? || params[:connector_url].present? ||
           params[:data_id].present? || params[:connectorUrl].present? ||
           params[:dataId].present?

          update_params = {}
          update_params['id']            = params[:id]
          update_params['data']          = Oj.dump(params[:data])
          update_params['data_id']       = params[:data_id] || params[:dataId]
          update_params['data_path']     = params[:data_path] || params[:dataPath]
          update_params['connector_url'] = params[:connector_url] || params[:connectorUrl]
          update_params
        else
          if params[:connector].present? && params[:connector][:data].present? && params[:connector][:connector_url].present?
            params.require(:connector).except(:dataset, :connector_url).permit!
          else
            params.require(:connector).except(:dataset).permit!
          end
        end
      end

      def overwritable
        unless params[:dataset].present? && params[:dataset][:data].present? && params[:dataset][:data][:attributes][:overwrite].present?
          render json: { errors: [{ status: 422, title: "Dataset data is locked and can't be updated" }] }, status: 422
        end
      end

      def success_notifier(status, message, status_code)
        notify(@dataset.id, status)
        render json: { success: true, message: message }, status: status_code
      end

      def fail_notifier(status, message)
        dataset_id = @dataset.present? ? @dataset.id : params['id']
        notify(dataset_id)
        render json: { success: false, message: message }, status: 422
      end

      def clone_url
        data = {}
        data['httpMethod'] = 'POST'
        data['url']        = "#{URI.parse(clone_uri)}"
        data['body']       = body_params
        data
      end

      def uri
        "#{@uri['api_gateway_url']}#{@uri['full_path']}"
      end

      def clone_uri
        "#{@uri['api_gateway_url']}/datasets/#{@dataset.id}/clone"
      end

      def body_params
        {
          "dataset" => {
            "datasetUrl" => "#{URI.parse(uri)}"
          },
          "application" => ["your", "apps"]
        }
      end

      def start_gc
        GC.start(full_mark: false, immediate_sweep: false)
      end
  end
end
