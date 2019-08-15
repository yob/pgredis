# coding: utf-8

RSpec.shared_examples "set" do
  context "SET" do
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
end

RSpec.shared_examples "get" do
  context "GET" do
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
end

