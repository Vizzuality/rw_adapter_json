# frozen_string_literal: true
class User < ActiveHash::Base
  fields :user_id, :role, :apps

  class << self
    def authorize_user!(user, dataset_apps, dataset_user_id=nil, match_apps=false)
      user_role     = user.role
      user_apps     = user.apps.sort         if user.apps.present?
      dataset_apps  = dataset_apps.uniq.sort if dataset_apps.present?
      if user_apps.present? && dataset_apps.present?
        update_case = user_apps.present? && dataset_apps.present? && match_apps.present? &&
                                            (user_apps & dataset_apps).any?
      end

      any_apps = if user_apps.present? && dataset_apps.present? && update_case
                   true
                 else
                   false
                 end

      user_dataset = user.user_id == dataset_user_id if dataset_user_id.present?

      case user_role
      when 'user' then false
      when 'manager'
        manager_ability(any_apps, user_dataset, dataset_user_id)
      when 'admin'
        admin_ability(any_apps)
      when 'superadmin'
        true
      else
        false
      end
    end

    def manager_ability(any_apps, user_dataset, dataset_user_id)
      if any_apps.present? && (user_dataset || dataset_user_id.blank?)
        true
      else
        false
      end
    end

    def admin_ability(any_apps)
      if any_apps.present?
        true
      else
        false
      end
    end
  end
end
