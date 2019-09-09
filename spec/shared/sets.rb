# coding: utf-8

RSpec.shared_examples "sets" do
  context "sadd" do
    context "when the set doesn't exist" do
      it "creates the set and returns 1" do
        expect(
          redis.sadd("foo","a")
        ).to eql(true)
      end
      it "adds the item to the set" do
        redis.sadd("foo","a")
        expect(redis.smembers("foo")).to match_array(["a"])
      end
    end
    context "when the set exists" do
      before do
        redis.sadd("foo","a")
      end
      context "adding a new item" do
        it "returns the number of items added" do
          expect(
            redis.sadd("foo","b")
          ).to eql(true)
        end
        it "adds the item to the set" do
          redis.sadd("foo","b")
          expect(redis.smembers("foo")).to match_array(["a","b"])
        end
      end

      context "adding two new items" do
        it "returns the number of items added" do
          expect(
            redis.sadd("foo",["b","c"])
          ).to eql(2)
        end
        it "adds the items to the set" do
          redis.sadd("foo",["b","c"])
          expect(redis.smembers("foo")).to match_array(["a","b","c"])
        end
      end

      context "adding one new and one existing item to the set" do
        it "returns the number of items added" do
          expect(
            redis.sadd("foo",["a","b"])
          ).to eql(1)
        end
        it "adds the new item to the set" do
          redis.sadd("foo",["a","b"])
          expect(redis.smembers("foo")).to match_array(["a","b"])
        end
      end

      context "adding an existing item to the set" do
        it "returns the number of items added" do
          expect(
            redis.sadd("foo","a")
          ).to eql(false)
        end
        it "doesn't modify the set" do
          redis.sadd("foo","a")
          expect(redis.smembers("foo")).to match_array(["a"])
        end
      end
    end
  end

  context "scard" do
    context "when the set doesn't exist" do
      it "returns 0" do
        expect(
          redis.scard("foo")
        ).to eql(0)
      end
    end
    context "when the set exists" do
      before do
          redis.sadd("foo","a")
          redis.sadd("foo","b")
          redis.sadd("foo","c")
      end
      it "returns the number of items in the set" do
        expect(
          redis.scard("foo")
        ).to eql(3)
      end
    end
  end

  context "smembers" do
    context "when the set doesn't exist" do
      it "returns an empty array" do
        expect(
          redis.smembers("foo")
        ).to eql([])
      end
    end
    context "when the set has 3 items" do
      before do
        redis.sadd("foo",["a","b","c"])
      end
      it "returns the items in an array" do
        expect(
          redis.smembers("foo")
        ).to match_array(["a","b","c"])
      end
    end
  end

  context "srem" do
    context "when the set doesn't exist" do
      it "returns 0" do
        expect(
          redis.srem("foo", "a")
        ).to eql(false)
      end
    end
    context "when the set has 3 items" do
      before do
        redis.sadd("foo",["a","b","c"])
      end
      context "removing two of them" do
        it "returns 2 and removes the items from the set" do
          expect(
            redis.srem("foo", ["a","b"])
          ).to eql(2)

          expect(
            redis.smembers("foo")
          ).to match_array(["c"])
        end
      end
    end
  end
end
