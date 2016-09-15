class ApplicationController < ActionController::API
  rescue_from ActiveRecord::RecordNotFound, with: :record_not_found

  protected

    def record_not_found
      render json: { errors: [{ status: '404', title: 'Record not found' }] } ,  status: 404
    end
end
