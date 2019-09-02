# coding: utf-8

RSpec.describe "realredis" do
  let(:redis) { Redis.new(url: ENV.fetch("REALREDIS_URL")) }

  include_examples "connection"
  include_examples "strings"
end

RSpec.describe "pgredis" do
  let(:redis) { Redis.new(url: ENV.fetch("PGREDIS_URL")) }

  include_examples "connection"
  include_examples "strings"
end
