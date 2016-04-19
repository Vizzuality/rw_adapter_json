module Filters
  class Order
    def self.apply_order(order_params)
      to_order = order_params.join(',').split(',')
      filter = ' ORDER BY'

      to_order.each_index do |i|
        order_attr = if to_order[i].include?('-')
                       "#{to_order[i].delete!('-')} DESC"
                     else
                       "#{to_order[i]} ASC"
                     end

        filter += ',' if i > 0
        filter += " #{order_attr}"
      end
      filter
    end
  end
end
