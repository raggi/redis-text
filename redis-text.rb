require 'rubygems'
require 'text'
require 'redis'
require 'digest'

class RedisText
  DMetaphone = lambda do |text|
    Text::Metaphone.double_metaphone(text).compact
  end
  MD5 = Digest::MD5

  attr_reader :redis, :encoder

  def initialize redis = Redis.new, encoder = DMetaphone
    @redis, @encoder = redis, encoder
  end

  def add id, document
    doc_id = add_id "doc_id", id

    words = encode document
    words.each do |word|
      word_id = add_id "word", word
      add_word word_id, doc_id
    end
  end

  def remove id, document
    doc_id = add_id "doc_id", id

    words = encode document
    words.each do |word|
      word_id = add_id "word", word
      remove_word word_id, doc_id
    end

    remove_string "doc_id", doc_id
    remove_id "doc_id", MD5.hexdigest(id.to_s)
  end

  def search text
    words = encode text
    word_keys = words.map do |word|
      word_id = add_id "word", word
      "contains/#{word_id}"
    end
    doc_ids = redis.sinter *word_keys
    doc_ids.map { |doc_id| get_string "doc_id", doc_id }
  end

  def encode text
    encoder.call text
  end

  def add_id type, string
    md5 = MD5.hexdigest string.to_s

    id = get_id type, md5
    return id if id

    id = next_id type
    add_string type, id, string

    unless atomic_set_id type, md5, id
      remove_string type, id
      retry
    end

    id.to_i
  end

  def get_id type, md5
    redis.get "#{type}/#{md5}/id"
  end

  def remove_id type, md5
    redis.del "#{type}/#{md5}/id"
  end

  def next_id type
    redis.incr "#{type}/next_id"
  end

  def add_string type, id, string
    redis.set "#{type}/#{id}/string", string
  end

  def remove_string type, id
    redis.del "#{type}/#{id}/string"
  end

  def get_string type, id
    redis.get "#{type}/#{id}/string"
  end

  def atomic_set_id type, md5, id
    redis.setnx "#{type}/#{md5}/id", id
  end

  def add_word word_id, doc_id
    redis.sadd "contains/#{word_id}", doc_id
  end

  def remove_word word_id, doc_id
    redis.srem "contains/#{word_id}", doc_id
  end

end

if __FILE__ == $0
  rt = RedisText.new
  rt.add 1, "one"
  rt.add 2, "two"
  rt.add 3, "three"
  rt.add 4, "four"

  # rt.add 5, "this is for showing how it works"
  p rt.search "fur"

  rt.remove 4, "four"

  p rt.search "fur"
end
