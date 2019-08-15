require 'redis'

RSpec.configure do |config|

  config.before(:each) do
    redis.flushall
  end
end
