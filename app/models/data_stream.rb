# frozen_string_literal: true
class DataStream
  CHUNK_SIZE = 1024 * 4

  attr_reader :data, :chunk_size

  def initialize(data, chunk_size = CHUNK_SIZE)
    @data       = data
    @chunk_size = chunk_size
  end

  def each
    while chunk = data.readpartial(chunk_size)
      yield chunk
    end
  rescue EOFError
    []
  ensure
    close
  end

  def close
    data.close
  end
end
