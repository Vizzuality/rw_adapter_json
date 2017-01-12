# frozen_string_literal: true
module Filters
  class Order
    def self.apply_order(order_params)
      to_order = order_params.split(',')
      filter = ' ORDER BY'

      to_order.each_index do |i|
        filter += ',' if i.positive?
        order_attr = if to_order[i].downcase.include?('desc') || to_order[i].downcase.include?('asc')
                       "#{to_order[i]}"
                     else
                       "#{to_order[i]} ASC"
                     end

        filter += " #{order_attr}"
      end
      filter
    end
  end
end
