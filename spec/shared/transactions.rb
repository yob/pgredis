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

      it "returns the results in an array" do
        result = redis.multi do
          redis.incr("foo")
          redis.incr("foo")
        end
        expect(result).to eql([2,3])
      end
    end
    context "when the key exists as a list" do
      before do
        redis.rpush("foo", "aaa")
      end
      context "when a command within the transaction returns an array" do
        it "returns the results in a nested array" do
          result = redis.multi do
            redis.lrange("foo", 0, 1)
          end
          expect(result).to eql([["aaa"]])
        end
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
