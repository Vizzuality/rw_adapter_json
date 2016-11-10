# frozen_string_literal: true
module HashFinder
  class ActiveSupport::HashWithIndifferentAccess
    def recursive_has_key?(key)
      key?(key) or values.any? { |v| v.is_a?(Hash) and v.recursive_has_key?(key) }
    end
  end
end
