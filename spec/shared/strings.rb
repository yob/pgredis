# coding: utf-8

RSpec.shared_examples "strings" do
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

  context  "with an existing key and short value" do
    before do
      redis.set("foo", "s1")
    end

    it "get returns the value" do
      expect(redis.get("foo")).to eql("s1")
    end
  end

  context  "with an existing key and value that has trailing whitespace" do
    before do
      redis.set("foo", "s1\n")
    end

    it "returns the value" do
      expect(redis.get("foo")).to eql("s1\n")
    end
  end

  context "when the key doesn't exist" do
    it "returns nil" do
      expect(redis.get("foo")).to eql(nil)
    end
  end
end

