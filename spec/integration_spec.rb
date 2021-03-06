# coding: utf-8

RSpec.describe "realredis" do
  let(:redis) { Redis.new(url: ENV.fetch("REALREDIS_URL")) }

  include_examples "keys"
  include_examples "connection"
  include_examples "strings"
  include_examples "lists"
  include_examples "sets"
  include_examples "sorted sets"
  include_examples "hashes"
  include_examples "pipelining"
  include_examples "server"
  include_examples "transactions"
end

RSpec.describe "pgredis" do
  let(:redis) { Redis.new(url: ENV.fetch("PGREDIS_URL")) }

  include_examples "keys"
  include_examples "connection"
  include_examples "strings"
  include_examples "lists"
  include_examples "sets"
  include_examples "sorted sets"
  include_examples "hashes"
  include_examples "pipelining"
  include_examples "server"
  include_examples "transactions"
end
