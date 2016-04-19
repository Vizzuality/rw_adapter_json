module V1
  class ConnectorsController < ApplicationController
    before_action :set_connector
    before_action :set_query_filter
    before_action :set_uri

    def show
      render json: @connector, serializer: ConnectorSerializer, query_filter: @query_filter, root: false, uri: @uri
    end

    private

      def set_connector
        @connector = JsonConnector.new(params) if params[:dataset].present?
      end

      def set_query_filter
        @query_filter = {}
        @query_filter['select'] = params[:select] if params[:select].present?
        @query_filter['order']  = params[:order]  if params[:order].present?
        # For Filter
        @query_filter['filter']     = params[:filter]     if params[:filter].present?
        @query_filter['filter_not'] = params[:filter_not] if params[:filter_not].present?
        # For group
        @query_filter['aggr_by']   = params[:aggr_by]   if params[:aggr_by].present?
        @query_filter['aggr_func'] = params[:aggr_func] if params[:aggr_func].present?
      end

      def api_gateway_url
        ENV['API_GATEWAY_URL'] || 'http://ec2-52-23-163-254.compute-1.amazonaws.com'
      end

      def set_uri
        @uri = {}
        @uri['api_gateway_url'] = api_gateway_url
        @uri['full_path']       = request.fullpath
      end
  end
end
