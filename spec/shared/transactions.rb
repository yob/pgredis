# coding: utf-8

RSpec.shared_examples "transactions" do
  context "multi with exec" do
    context "when the key exists" do
      before do
        redis.incr("foo")
      end
      it "applies both increments" do
        redis.multi
        expect(redis.incr("foo")).to eql("QUEUED")
        expect(redis.incr("foo")).to eql("QUEUED")
        redis.exec
        expect(redis.get("foo")).to eql("3")
      end
    end
  end

  context "multi with discard" do
    context "when the key exists" do
      before do
        redis.incr("foo")
      end
      it "applies no changes" do
        redis.multi
        expect(redis.incr("foo")).to eql("QUEUED")
        redis.discard
        expect(redis.get("foo")).to eql("1")
      end
    end
  end
end
