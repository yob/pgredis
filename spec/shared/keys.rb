# coding: utf-8

RSpec.shared_examples "keys" do
  context "expire" do
    context "when the key exists" do
      before do
        redis.set("foo", 1)
        expect(redis.ttl("foo")).to eql(-1)
      end
      it "returns 1" do
        expect(
          redis.expire("foo", 10)
        ).to eql(true)
      end

      it "sets an expiry on the key" do
        redis.expire("foo", 10)
        expect(redis.ttl("foo")).to be_between(0, 10)
      end
    end

    context "when the key already exists but it's expired" do
      before do
        redis.set("foo", "bar", px: 1) # almost insta expire
        sleep(0.1)
      end

      it "returns 0 and doesn't resurrect the key" do
        expect(
          redis.expire("foo", 10)
        ).to eql(false)
        expect(
          redis.get("foo")
        ).to eql(nil)
      end
    end

    context "when the key does not exist" do
      it "returns 0" do
        expect(
          redis.expire("foo", 10)
        ).to eql(false)
      end
    end
  end
end
