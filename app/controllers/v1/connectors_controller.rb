module V1
  class ConnectorsController < ApplicationController
    include ActionController::Live

    FLUSH_EVERY = 500

    before_action :disable_gc,       only:   :show
    before_action :set_connector,    except: :info
    before_action :set_query_filter, except: :info
    before_action :set_uri,          except: :info
    before_action :set_dataset,      only: [:show, :update, :update_data, :overwrite, :destroy, :delete_data]
    before_action :set_data,         only:   :show
    after_action  :enable_gc,        only:   :show

    def show
      render json: @connector, serializer: ConnectorSerializer, query_filter: @query_filter, root: false, uri: @uri, data: @data
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
        Dataset.notifier(params[:id], 'deleted') if ServiceSetting.auth_token.present?
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
        @dataset = Dataset.select(:id).where(id: params[:id]).first
      end

      def set_data
        @data = @connector.data(@query_filter)
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
        Dataset.notifier(dataset_id, status) if ServiceSetting.auth_token.present?
      end

      def meta_data_params
        @connector.recive_dataset_meta[:dataset]
      end

      def connector_params
        params.require(:connector).permit!
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

      def stream_data_array(data)
        return data if Rails.env.test?
        headers["Content-Disposition"] = 'inline'
        headers["Content-Type"]        = 'application/json; charset=utf-8'
        # headers["Content-Encoding"]    = 'deflate'

        # deflate = Zlib::Deflate.new

        buffer = "{\n"
        buffer << '"cloneUrl": '
        buffer << JSON.pretty_generate(clone_url)
        buffer << ",\n"
        buffer << '"data": '
        buffer << "[\n  "

        data.each_with_index do |object, i|
          buffer << ",\n  " unless i.zero?
          buffer << JSON.pretty_generate(object, depth: 1)

          if (i % FLUSH_EVERY).zero?
            # write(deflate, buffer)
            write(buffer)
            buffer = ""
          end
        end

        buffer << "\n]\n}\n"

        write(buffer)
        # write(deflate, buffer)
        # write(deflate, nil) # Flush deflate.
        response.stream.close
      end

      def write(data)
        response.stream.write(data)
      end

      # def write(deflate, data)
      #   deflated = deflate.deflate(data)
      #   response.stream.write(deflated)
      # end

      def disable_gc
        GC.start(full_mark: false, immediate_sweep: false)
      end

      def enable_gc
        response.stream.close
        GC.start(full_mark: false, immediate_sweep: false)
      end
  end
end
