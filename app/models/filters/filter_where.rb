module Filters
  OPERATOR        = [['NOT IN','<','<=','>','>=','NOT BETWEEN'],['IN','>=','>','<=','<','BETWEEN']]
  OPERATOR_PARAMS = ['==','>=','>>','<=','<<','><']

  class FilterWhere
    class << self
      def apply_where(filter, not_filter)
        filter_params = not_filter.present? ? not_filter : filter
        operator      = not_filter.present? ? OPERATOR[0] : OPERATOR[1]

        to_filter = if filter_params.present? && (filter_params.include?('<and>') || filter_params.include?('<or>'))
                      filter_params.split(/<and>|<or>/)
                    elsif filter_params.present?
                      filter_params.partition(' ')
                    end

        filter_attr = []
        filter_val  = []

        5.times do |n|
          filter_attr << filter_attr_val(filter_params, to_filter, n, 0)
          filter_val  << filter_attr_val(filter_params, to_filter, n, 1)
        end

        filter_attr_btw = filter_attr_val(filter_params, to_filter, 5, 0)
        filter_val_btw  = to_filter.map { |p| p.split('><') }.delete_if { |p| p.size < 2 }.reject(&:empty?).map { |p| p[1].split('..') } if filter_params.include?('><')

        filter = ''

        5.times do |n|
          if filter_attr[n].present?
            filter_attr[n].each_index do |i|
              cast_type = is_numeric?(filter_val[n][i]) ? 'int' : 'text'

              filter += ' AND' if i > 0
              filter += " #{filter_attr[n][i]}::#{cast_type} #{operator[n]} (#{filter_val[n][i]})"
              filter += ' AND' if filter_attr.compact.size > 1
            end
          end
        end

        filter = filter.split(' ')[0...-1].join(' ') if filter.split(' ')[-1] == 'AND'
        filter += ' AND' if filter_attr.compact.size > 0 && filter_attr_btw.present?

        if filter_attr_btw.present?
          filter_attr_btw.each_index do |i|
            cast_type = is_numeric?(filter_val_btw[i][0]) ? 'int' : 'text'

            filter += ' AND' if i > 0
            filter += " #{filter_attr_btw[i]}::#{cast_type} #{operator[5]} #{filter_val_btw[i][0]} AND #{filter_val_btw[i][1]}"
          end
        end
        filter
      end

      def filter_attr_val(filter_params, to_filter, n, obj)
        (filter_params.include?(OPERATOR_PARAMS[n]) ? to_filter.map { |p| p.split(OPERATOR_PARAMS[n]) }.delete_if { |p| p.size < 2 }.reject(&:empty?).map { |p| p[obj] } : nil)
      end

      def is_numeric?(expr)
        !Float("#{expr}".first).nil? rescue false
      end
    end
  end
end
