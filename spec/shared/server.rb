# coding: utf-8

RSpec.shared_examples "server" do
  context "dbsize" do
    context "with an empty database" do
      it "returns 0" do
        expect(redis.dbsize).to eql(0)
      end
    end
    context "with some database content" do
      before do
        redis.set("a", 1)
        redis.lpush("b", 2)
        redis.sadd("c", 3)
        redis.zadd("d", 1, 4)
        redis.hset("e", "f", 5)
      end
      it "returns 5" do
        expect(redis.dbsize).to eql(5)
      end
    end
  end

  context "info" do
    context "with no arg" do
      it "returns data separated by newlines (converted to a Hash by redis-rb)" do
        result = redis.info
        expect(result).to be_a(Hash)
        expect(result.keys).to include("redis_version")
      end
    end
    context "with an arg" do
      it "returns just the info for that section"
    end

  end

  context "client setname" do
    it "returns data separated by newlines (converted to a Hash by redis-rb)" do
      expect(
        redis.client("setname", "foo")
      ).to eql("OK")
    end

  end
end
