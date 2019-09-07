# coding: utf-8

RSpec.shared_examples "lists" do
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

      context "negative start and end" do
        it "returns an empty array" do
          expect(
            redis.lrange("foo", -3, -1)
          ).to eql(["a", "b", "c"])
        end
      end

      context "last 2 items on the list using negative start and end" do
        it "returns an empty array" do
          expect(
            redis.lrange("foo", -2, -1)
          ).to eql(["b", "c"])
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
        it "returns the new size" do
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
