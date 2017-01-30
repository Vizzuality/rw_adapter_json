# frozen_string_literal: true
module Authorization
  extend ActiveSupport::Concern

  included do
    before_action :set_user,       except: [:show, :fields, :create, :destroy]
    before_action :authorize_user, except: [:show, :fields, :create, :destroy]

    private

      def set_user
        if ENV.key?('OLD_GATEWAY') && ENV.fetch('OLD_GATEWAY').include?('true')
          User.data = [{ user_id: '123-123-123', role: 'superadmin', apps: nil }]
          @user = User.last
        elsif params[:loggedUser].present? && params[:loggedUser][:id] != 'microservice'
          user_id       = params[:loggedUser][:id]
          @role         = params[:loggedUser][:role].downcase
          @apps         = if @role.include?('superadmin')
                            ['AllApps']
                          elsif params[:loggedUser][:extraUserData].present? && params[:loggedUser][:extraUserData][:apps].present?
                            params[:loggedUser][:extraUserData][:apps].map { |v| v.downcase }.uniq
                          end
          @dataset_apps = dataset_params[:application]

          User.data = [{ user_id: user_id, role: @role, apps: @apps }]
          @user = User.last
        else
          render json: { errors: [{ status: 401, title: 'Not authorized!' }] }, status: 401 if params[:loggedUser][:id] != 'microservice'
        end
      end

      def authorize_user
        @authorized = User.authorize_user!(@user, dataset_params[:application], dataset_params[:userId], match_apps: true)

        if @authorized.blank?
          render json: { errors: [{ status: 401, title: 'Not authorized!' }] }, status: 401
        end
      end

      def dataset_params
        params.require(:dataset).permit!
      end
  end

  class_methods {}
end
