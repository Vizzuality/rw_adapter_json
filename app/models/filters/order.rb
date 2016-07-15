module Filters
  class Order
    def self.apply_order(order_params)
      to_order = order_params.split(',')
      filter = ' ORDER BY'

      to_order.each_index do |i|
        filter += ',' if i > 0
        order_attr = if to_order[i].include?('DESC') || to_order[i].include?('ASC')
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
