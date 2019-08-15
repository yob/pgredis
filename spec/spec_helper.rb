require 'redis'

Dir[File.dirname(__FILE__) + "/shared/**/*.rb"].each {|f| require f}

RSpec.configure do |config|
  config.before(:each) do
    redis.flushall
  end
end
