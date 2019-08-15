# coding: utf-8

RSpec.shared_examples "set" do
  context "setting a short string" do
    it "returns OK" do
      expect(
        redis.set("foo", "s1")
      ).to eql("OK")
    end
  end
  context "setting nil" do
    it "returns OK" do
      expect(
        redis.set("foo",nil) 
      ).to eql("OK")
    end
  end
end

RSpec.shared_examples "get" do
  context  "when the key exists" do
    before do
      redis.set("foo", "s1")
    end

    it "returns the value" do
      expect(redis.get("foo")).to eql("s1")
    end
  end

  context  "when the key doesn't exist" do
    it "returns nil" do
      expect(redis.get("foo")).to eql(nil)
    end
  end
end


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
