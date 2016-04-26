module V1
  class ConnectorsController < ApplicationController
    before_action :set_connector
    before_action :set_query_filter
    before_action :set_uri

    def show
      render json: @connector, serializer: ConnectorSerializer, query_filter: @query_filter, root: false, uri: @uri
    end

    def create
      # begin
      #   @dataset = JsonConnector.build_dataset(connector_params)
      #   @dataset.save
      #   render json: { success: true, message: 'Dataset created' }, status: 201
      #   notify('saved')
      # rescue
      #   render json: { success: false, message: 'Error creating dataset' }, status: 422
      #   notify
      # end
      @dataset = JsonConnector.build_dataset(connector_params)
        @dataset.save
        render json: { success: true, message: 'Dataset created' }, status: 201
        notify('saved')
    end

    private

      def set_connector
        @connector = JsonConnector.new(params) if params[:dataset].present? || params[:connector].present?
      end

      def set_query_filter
        @query_filter = {}
        @query_filter['select']     = params[:select] if params[:select].present?
        @query_filter['order']      = params[:order]  if params[:order].present?
        # For Filter
        @query_filter['filter']     = params[:filter]     if params[:filter].present?
        @query_filter['filter_not'] = params[:filter_not] if params[:filter_not].present?
        # For group
        @query_filter['aggr_by']    = params[:aggr_by]   if params[:aggr_by].present?
        @query_filter['aggr_func']  = params[:aggr_func] if params[:aggr_func].present?
      end

      def set_uri
        @uri = {}
        @uri['api_gateway_url'] = ENV['API_GATEWAY_URL'] if ENV['API_GATEWAY_URL'].present?
        @uri['full_path']       = request.fullpath
      end

      def notify(status=nil)
        Dataset.notifier(connector_params['id'], status) if ENV['API_DATASET_META_URL'].present?
      end

      def meta_data_params
        @connector.recive_dataset_meta[:dataset]
      end

      def connector_params
        params.require(:connector).permit!
      end
  end
end
