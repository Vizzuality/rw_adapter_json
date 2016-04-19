module ReadOnlyModel
  def readonly?
    true
  end

  def delete
    raise ReadOnlyRecord
  end
end
