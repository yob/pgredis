# coding: utf-8

RSpec.describe "realredis" do
  let(:redis) { Redis.new(url: ENV.fetch("REALREDIS_URL")) }

  include_examples "set"
  include_examples "get"
end

RSpec.describe "pgredis" do
  let(:redis) { Redis.new(url: ENV.fetch("PGREDIS_URL")) }

  include_examples "set"
  include_examples "get"
end
