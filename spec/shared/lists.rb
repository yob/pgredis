# coding: utf-8

RSpec.shared_examples "lists" do
  context "brpop" do
    context "when no lists exist" do
      context "removing the last item" do
        it "returns nil" do
          expect(
            redis.brpop("foo", timeout: 1)
          ).to eql(nil)
        end
      end
      context "removing the last item from two lists" do
        it "returns nil" do
          expect(
            redis.brpop(["foo", "pop"], timeout: 1)
          ).to eql(nil)
        end
      end
    end
    context "when a list exists with a two item" do
      before do
        redis.rpush("foo", ["aaa", "bbb"])
      end

      context "removing a single item" do
        it "returns the final items and removes the item from the list" do
          expect(
            redis.brpop("foo", timeout: 1)
          ).to eql(["foo", "bbb"])
          expect(
            redis.lrange("foo", 0, -1)
          ).to eql(["aaa"])
        end
      end
    end

    context "when a list exists with an item each" do
      before do
        redis.rpush("foo", "aaa")
        redis.rpush("bar", "bbb")
      end

      context "removing a single item from both lists" do
        it "returns the corcect item and removes it from the list" do
          expect(
            redis.brpop(["foo","bar"], timeout: 1)
          ).to eql(["foo", "aaa"])
          expect(
            redis.lrange("foo", 0, -1)
          ).to eql([])
          expect(
            redis.lrange("bar", 0, -1)
          ).to eql(["bbb"])
        end
      end
    end
  end

  context "llen" do
    context "when the list doesn't exist" do
      it "returns 0" do
        expect(
          redis.llen("foo")
        ).to eql(0)
      end
    end
    context "when the list exists with a single item" do
      before do
        redis.rpush("foo", "bar")
      end
      it "returns the list size" do
        expect(
          redis.llen("foo")
        ).to eql(1)
      end
    end
  end

  context "lpop" do
    context "when the list doesn't exist" do
      context "removing the last item" do
        it "returns nil" do
          expect(
            redis.lpop("foo")
          ).to eql(nil)
        end
      end
    end
    context "when the list exists with a two item" do
      before do
        redis.lpush("foo", ["aaa", "bbb"])
      end

      context "removing a single item" do
        it "returns the final items and removes the item from the list" do
          expect(
            redis.lpop("foo")
          ).to eql("bbb")
          expect(
            redis.lrange("foo", 0, -1)
          ).to eql(["aaa"])
        end
      end

    end
  end

  context "lpush" do
    context "when the list doesn't exist" do
      context "pushing a single item" do
        it "creates the list on demand and returns the new size" do
          expect(
            redis.lpush("foo", "bar")
          ).to eql(1)
        end
      end
    end
    context "when the list exists with a single item" do
      before do
        redis.lpush("foo", "bar")
      end

      context "pushing a single item" do
        it "returns the new size" do
          expect(
            redis.lpush("foo", "baz")
          ).to eql(2)
        end
        it "appends the new value to the start of the list" do
          redis.lpush("foo", "baz")
          expect(
            redis.lrange("foo", 0, -1)
          ).to eql(["baz", "bar"])
        end
      end

      context "pushing multiple items" do
        it "returns the new size" do
          expect(
            redis.lpush("foo", ["baz", "boo"])
          ).to eql(3)
        end
        it "appends the new value to the start of the list" do
          redis.lpush("foo", ["baz", "boo"])
          expect(
            redis.lrange("foo", 0, -1)
          ).to eql(["boo", "baz", "bar"])
        end
      end
    end

  end

  context "lrange" do
    context "when the list doesn't exist" do
      context "reading the first item" do
        it "returns an empty array" do
          expect(
            redis.lrange("foo", 0, 0)
          ).to eql([])
        end
      end
    end

    context "when the list has 3 items" do
      before do
        redis.rpush("foo", "a")
        redis.rpush("foo", "b")
        redis.rpush("foo", "c")
      end
      context "reading the first item" do
        it "returns an array with 1 item" do
          expect(
            redis.lrange("foo", 0, 0)
          ).to eql(["a"])
        end
      end

      context "reading the full list" do
        it "returns an array" do
          expect(
            redis.lrange("foo", 0, 2)
          ).to eql(["a","b","c"])
        end
      end

      context "reading the full list using a negative index" do
        it "returns an array" do
          expect(
            redis.lrange("foo", 0, -1)
          ).to eql(["a","b","c"])
        end
      end

      context "skipping the first item" do
        it "returns an array" do
          expect(
            redis.lrange("foo", 1, 2)
          ).to eql(["b","c"])
        end
      end

      context "reading the first two items" do
        it "returns an array" do
          expect(
            redis.lrange("foo", 0, 1)
          ).to eql(["a","b"])
        end
      end

      context "reading past the end of the list" do
        it "returns an array" do
          expect(
            redis.lrange("foo", 0, 10)
          ).to eql(["a","b","c"])
        end
      end

      context "starting past the end of the list" do
        it "returns an empty array" do
          expect(
            redis.lrange("foo", 10, 20)
          ).to eql([])
        end
      end

      context "starting before the start of the list" do
        it "returns an empty array" do
          expect(
            redis.lrange("foo", -20, -10)
          ).to eql([])
        end
      end

      context "negative start and end" do
        it "returns an empty array" do
          expect(
            redis.lrange("foo", -3, -1)
          ).to eql(["a", "b", "c"])
        end
      end

      context "last 2 items on the list using negative start and end" do
        it "returns an array" do
          expect(
            redis.lrange("foo", -2, -1)
          ).to eql(["b", "c"])
        end
      end
    end
  end

  context "lrem" do
    context "when the list doesn't exist" do
      context "removing a single item" do
        it "returns 0 - we removed no items from an imaginary list" do
          expect(
            redis.lrem("foo", 1, "bar")
          ).to eql(0)
        end
      end
    end
    context "when the list exists with a two identical item" do
      before do
        redis.lpush("foo", ["bar", "bar"])
      end

      context "removing a single item" do
        it "returns the number of removed items" do
          expect(
            redis.lrem("foo", 1, "bar")
          ).to eql(1)
        end
        it "removes the item from the list" do
          redis.lrem("foo", 1, "bar")
          expect(
            redis.lrange("foo", 0, -1)
          ).to eql(["bar"])
        end
      end

      context "removing both items" do
        it "returns the number of removed items" do
          expect(
            redis.lrem("foo", 2, "bar")
          ).to eql(2)
        end
        it "removes the item from the list" do
          redis.lrem("foo", 2, "bar")
          expect(
            redis.lrange("foo", 0, -1)
          ).to eql([])
        end
      end

      context "removing an item that isn't in the list" do
        it "returns the number of removed items" do
          expect(
            redis.lrem("foo", 1, "aaa")
          ).to eql(0)
        end
        it "removes no items from the list" do
          redis.lrem("foo", 1, "aaa")
          expect(
            redis.lrange("foo", 0, -1)
          ).to eql(["bar","bar"])
        end
      end
    end
  end

  context "rpop" do
    context "when the list doesn't exist" do
      context "removing the last item" do
        it "returns nil" do
          expect(
            redis.rpop("foo")
          ).to eql(nil)
        end
      end
    end
    context "when the list exists with a two item" do
      before do
        redis.lpush("foo", ["aaa", "bbb"])
      end

      context "removing a single item" do
        it "returns the final items and removes the item from the list" do
          expect(
            redis.rpop("foo")
          ).to eql("aaa")
          expect(
            redis.lrange("foo", 0, -1)
          ).to eql(["bbb"])
        end
      end

    end
  end

  context "rpush" do
    context "when the list doesn't exist" do
      context "pushing a single item" do
        it "creates the list on demand and returns the new size" do
          expect(
            redis.rpush("foo", "bar")
          ).to eql(1)
        end
      end
    end
    context "when the list exists with a single item" do
      before do
        redis.rpush("foo", "bar")
      end

      context "pushing a single item" do
        it "RETURns the new size" do
          expect(
            redis.rpush("foo", "baz")
          ).to eql(2)
        end
        it "appends the new value to the end of the list" do
          redis.rpush("foo", "baz")
          expect(
            redis.lrange("foo", 0, -1)
          ).to eql(["bar","baz"])
        end
      end

      context "pushing multiple items" do
        it "returns the new size" do
          expect(
            redis.rpush("foo", ["baz", "boo"])
          ).to eql(3)
        end
        it "appends the new values to the end of the list" do
          redis.rpush("foo", ["baz", "boo"])
          expect(
            redis.lrange("foo", 0, -1)
          ).to eql(["bar","baz","boo"])
        end
      end
    end

  end

end
