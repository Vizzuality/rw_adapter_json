class Dataset < ApplicationRecord
  self.table_name = :datasets

  include ReadOnlyModel
  include NullAttributesRemover

  belongs_to :dateable, polymorphic: true
end
