# coding: utf-8

RSpec.shared_examples "sorted sets" do
  context "zadd" do
    context "when the set doesn't exist" do
      it "creates the set and returns 1" do
        expect(
          redis.zadd("foo","1.1", "a")
        ).to eql(true)
      end
      it "adds the item to the set" do
        redis.zadd("foo","1.1", "a")
        expect(
          redis.zrange("foo",0,1, with_scores: true)
        ).to eql([
          ["a", 1.1]
        ])
      end
    end
    context "when the set exists" do
      before do
        redis.zadd("foo","1.1","a")
      end
      context "adding a new item" do
        it "returns the number of items added" do
          expect(
            redis.zadd("foo","1.0","b")
          ).to eql(true)
        end
        it "adds the item to the set" do
          redis.zadd("foo","1.0", "b")
          expect(
            redis.zrange("foo",0,2, with_scores: true)
          ).to eql([
            ["b", 1.0],
            ["a", 1.1]
          ])
        end
      end
      context "adding a multiple new items" do
        it "returns the number of items added" do
          expect(
            redis.zadd("foo",[["1.0","b"],["2.0","c"]])
          ).to eql(2)
        end
        it "adds the item to the set" do
          redis.zadd("foo",[["1.0","b"],["2.0","c"]])
          expect(
            redis.zrange("foo",0,2, with_scores: true)
          ).to eql([
            ["b", 1.0],
            ["a", 1.1],
            ["c", 2.0],
          ])
        end
      end
      context "adding an existing item with an identical score" do
        it "returns the number of items added" do
          expect(
            redis.zadd("foo","1.1","a")
          ).to eql(false)
        end
        it "does not modify the set" do
          redis.zadd("foo","1.1","a")
          expect(
            redis.zrange("foo",0,2, with_scores: true)
          ).to eql([
            ["a", 1.1]
          ])
        end
      end
      context "adding an existing item with a newscore" do
        it "returns the number of items added, exlcuding items that only had the score updated" do
          expect(
            redis.zadd("foo","1.2","a")
          ).to eql(false)
        end
        it "updates the score of the existing item" do
          redis.zadd("foo","1.2", "a")
          expect(
            redis.zrange("foo",0,2, with_scores: true)
          ).to eql([
            ["a", 1.2]
          ])
        end
      end
      context "with XX option" do
        context "adding an item that is already in the set" do
          it "updates the item"
        end
        context "adding an item that is not in the set" do
          it "does not add the item"
        end
      end
      context "with NX option" do
        context "adding an item that is already in the set" do
          it "does not update the item"
        end
        context "adding an item that is not in the set" do
          it "adds the item"
        end
      end
      context "with CH option" do
        context "adding a new item" do
          it "returns the number of items added or updated" do
            expect(
              redis.zadd("foo","1.0","b", ch: true)
            ).to eql(true)
          end
        end
        context "adding an existing item with an identical score" do
          it "returns the number of items added or updated" do
            expect(
              redis.zadd("foo","1.1","a", ch: true)
            ).to eql(false)
          end
        end
        context "adding an existing item with a newscore" do
          it "returns the number of items added, including items that only had the score updated" do
            expect(
              redis.zadd("foo","1.2","a", ch: true)
            ).to eql(true)
          end
        end
      end
      context "with INCR option" do
        context "adding a new item" do
          it "returns the score of the item added" # do
          #  expect(
          #    redis.zadd("foo","1.0","b", incr: true)
          #  ).to eql(1.0)
          #end
        end
        context "incrementing an existing item" do
          it "returns the new score of the item" # do
          #  expect(
          #    redis.zadd("foo","1.0","a", incr: true)
          #  ).to eql(2.1)
          #end
        end
      end
    end
  end
  context "zcard" do
    context "when the set doesn't exist" do
      it "returns 0" do
        expect(
          redis.zcard("foo")
        ).to eql(0)
      end
    end
    context "when the set exists" do
      before do
          redis.zadd("foo","1", "a")
          redis.zadd("foo","2", "b")
          redis.zadd("foo","3", "c")
      end
      it "returns the number of items in the set" do
        expect(
          redis.zcard("foo")
        ).to eql(3)
      end
    end
  end

  context "zrange" do
    context "when the zset doesn't exist" do
      context "reading the first item" do
        it "returns an empty array" do
          expect(
            redis.zrange("foo", 0, 0)
          ).to eql([])
        end
      end
    end

    context "when the list has 3 items" do
      before do
        redis.zadd("foo", 2, "b")
        redis.zadd("foo", 1, "a")
        redis.zadd("foo", 3, "c")
      end
      context "reading the first item" do
        it "returns an array with 1 item" do
          expect(
            redis.zrange("foo", 0, 0)
          ).to eql(["a"])
        end
      end

      context "reading the full set" do
        it "returns an array" do
          expect(
            redis.zrange("foo", 0, 2)
          ).to eql(["a","b","c"])
        end
      end

      context "reading the full set using a negative index" do
        it "returns an array" do
          expect(
            redis.zrange("foo", 0, -1)
          ).to eql(["a","b","c"])
        end
      end

      context "reading the fist item using a negative index" do
        it "returns an array" do
          expect(
            redis.zrange("foo", 0, -3)
          ).to eql(["a"])
        end
      end

      context "reading no items using a negative index" do
        it "returns an empty array" do
          expect(
            redis.zrange("foo", 0, -4)
          ).to eql([])
        end
      end

      context "skipping the first item" do
        it "returns an array" do
          expect(
            redis.zrange("foo", 1, 2)
          ).to eql(["b","c"])
        end
      end

      context "reading the first two items" do
        it "returns an array" do
          expect(
            redis.zrange("foo", 0, 1)
          ).to eql(["a","b"])
        end
      end

      context "reading past the end of the set" do
        it "returns an array" do
          expect(
            redis.zrange("foo", 0, 10)
          ).to eql(["a","b","c"])
        end
      end

      context "starting past the end of the set" do
        it "returns an empty array" do
          expect(
            redis.zrange("foo", 10, 20)
          ).to eql([])
        end
      end

      context "starting before the start of the set" do
        it "returns an empty array" do
          expect(
            redis.zrange("foo", -20, -10)
          ).to eql([])
        end
      end

      context "negative start and end" do
        it "returns an empty array" do
          expect(
            redis.zrange("foo", -3, -1)
          ).to eql(["a", "b", "c"])
        end
      end

      context "last 2 items of the set using negative start and end" do
        it "returns an array" do
          expect(
            redis.zrange("foo", -2, -1)
          ).to eql(["b", "c"])
        end
      end

      context "when WITHSCORES option is used" do
        context "reading the full set" do
          it "returns an array that includes the scores" do
            expect(
              redis.zrange("foo", 0, 2, with_scores: true)
            ).to eql([
              ["a",1.0],
              ["b",2.0],
              ["c",3.0]
            ])
          end
        end
      end
    end
    context "when the list has 3 items with the same score" do
      before do
        redis.zadd("foo", 1, "b")
        redis.zadd("foo", 1, "a")
        redis.zadd("foo", 1, "c")
      end
      context "reading full set" do
        it "returns an array with all items sorted lexigraphically" do
          expect(
            redis.zrange("foo", 0, 2)
          ).to eql(["a","b","c"])
        end
      end
    end
  end
  context "zrangebyscore" do
    context "when the set doesn't exist" do
      it "returns empty array" do
        expect(
          redis.zrangebyscore("foo", 0, 1)
        ).to eql([])
      end
    end
    context "when the set has 3 items" do
      before do
        redis.zadd("foo","2","b")
        redis.zadd("foo","1","a")
        redis.zadd("foo","3","c")
      end
      context "returning two of them with inclusive syntax" do
        it "returns the items" do
          expect(
            redis.zrangebyscore("foo", 1, 2)
          ).to eql(["a","b"])
        end
      end
      context "returning one of them with exclusive syntax" do
        it "returns the item" do
          expect(
            redis.zrangebyscore("foo", "1", "(2")
          ).to eql(["a"])
        end
      end
      context "returning one of them with negative infinite syntax" do
        it "returns the item" do
          expect(
            redis.zrangebyscore("foo", "-inf", "1")
          ).to eql(["a"])
        end
      end
      context "returning one of them with positive infinite syntax" do
        it "returns the item" do
          expect(
            redis.zrangebyscore("foo", "3", "+inf")
          ).to eql(["c"])
        end
      end
      context "when WITHSCORES option is used" do
        context "reading the full set" do
          it "returns an array that includes the scores" do
            expect(
              redis.zrangebyscore("foo", 1, 3, with_scores: true)
            ).to eql([
              ["a",1.0],
              ["b",2.0],
              ["c",3.0]
            ])
          end
        end
      end
      context "when LIMIT option is used" do
        context "reading a partial set" do
          it "returns the correct items" do
            expect(
              redis.zrangebyscore("foo", "-inf", "+inf", limit: [1,2])
            ).to eql(["b","c"])
          end
        end
      end
    end
    context "when the set has 3 items with float values" do
      before do
        redis.zadd("foo","1.2","b")
        redis.zadd("foo","1.1","a")
        redis.zadd("foo","1.3","c")
      end
      context "returning two of them with inclusive syntax" do
        it "returns the items" do
          expect(
            redis.zrangebyscore("foo", "1.1", "1.2")
          ).to eql(["a","b"])
        end
      end
    end
  end
  context "zrevrange" do
    context "when the zset doesn't exist" do
      context "reading the first item" do
        it "returns an empty array" do
          expect(
            redis.zrevrange("foo", 0, 0)
          ).to eql([])
        end
      end
    end

    context "when the list has 3 items" do
      before do
        redis.zadd("foo", 2, "b")
        redis.zadd("foo", 1, "a")
        redis.zadd("foo", 3, "c")
      end
      context "reading the first item" do
        it "returns an array with 1 item" do
          expect(
            redis.zrevrange("foo", 0, 0)
          ).to eql(["c"])
        end
      end

      context "reading the full set" do
        it "returns an array" do
          expect(
            redis.zrevrange("foo", 0, 2)
          ).to eql(["c","b","a"])
        end
      end

      context "reading the full set using a negative index" do
        it "returns an array" do
          expect(
            redis.zrevrange("foo", 0, -1)
          ).to eql(["c","b","a"])
        end
      end

      context "skipping the first item" do
        it "returns an array" do
          expect(
            redis.zrevrange("foo", 1, 2)
          ).to eql(["b","a"])
        end
      end

      context "reading the first two items" do
        it "returns an array" do
          expect(
            redis.zrevrange("foo", 0, 1)
          ).to eql(["c","b"])
        end
      end

      context "reading past the end of the set" do
        it "returns an array" do
          expect(
            redis.zrevrange("foo", 0, 10)
          ).to eql(["c","b","a"])
        end
      end

      context "starting past the end of the set" do
        it "returns an empty array" do
          expect(
            redis.zrevrange("foo", 10, 20)
          ).to eql([])
        end
      end

      context "starting before the start of the set" do
        it "returns an empty array" do
          expect(
            redis.zrevrange("foo", -20, -10)
          ).to eql([])
        end
      end

      context "negative start and end" do
        it "returns an empty array" do
          expect(
            redis.zrevrange("foo", -3, -1)
          ).to eql(["c", "b", "a"])
        end
      end

      context "last 2 items of the set using negative start and end" do
        it "returns an array" do
          expect(
            redis.zrevrange("foo", -2, -1)
          ).to eql(["b", "a"])
        end
      end

      context "when WITHSCORES option is used" do
        context "reading the full set" do
          it "returns an array that includes the scores" do
            expect(
              redis.zrevrange("foo", 0, 2, with_scores: true)
            ).to eql([
              ["c",3.0],
              ["b",2.0],
              ["a",1.0],
            ])
          end
        end
      end
    end
    context "when the list has 3 items with the same score" do
      before do
        redis.zadd("foo", 1, "b")
        redis.zadd("foo", 1, "a")
        redis.zadd("foo", 1, "c")
      end
      context "reading full set" do
        it "returns an array with all items sorted reverse lexigraphically" do
          expect(
            redis.zrevrange("foo", 0, 2)
          ).to eql(["c","b","a"])
        end
      end
    end
  end
  context "zrem" do
    context "when the set doesn't exist" do
      it "returns 0" do
        expect(
          redis.zrem("foo", "a")
        ).to eql(false)
      end
    end
    context "when the set has 3 items" do
      before do
        redis.zadd("foo","2","b")
        redis.zadd("foo","1","a")
        redis.zadd("foo","3","c")
      end
      context "removing two of them" do
        it "returns 2 and removes the items from the set" do
          expect(
            redis.zrem("foo", ["a","b"])
          ).to eql(2)

          expect(
            redis.zrange("foo",0,1, with_scores: true)
          ).to eql([
            ["c", 3.0]
          ])
        end
      end
    end
  end
  context "zremrangebyrank" do
    context "when the set doesn't exist" do
      it "returns 0" do
        expect(
          redis.zremrangebyrank("foo", 0, 1)
        ).to eql(0)
      end
    end
    context "when the set has 3 items" do
      before do
        redis.zadd("foo","2","b")
        redis.zadd("foo","1","a")
        redis.zadd("foo","3","c")
      end
      context "removing two of them" do
        it "returns 2 and removes the items from the set" do
          expect(
            redis.zremrangebyrank("foo", 0, 1)
          ).to eql(2)

          expect(
            redis.zrange("foo",0,2, with_scores: true)
          ).to eql([
            ["c", 3.0]
          ])
        end
      end
      context "removing the middle one" do
        it "returns 1 and removes the item from the set" do
          expect(
            redis.zremrangebyrank("foo", 1, 1)
          ).to eql(1)

          expect(
            redis.zrange("foo",0,2, with_scores: true)
          ).to eql([
            ["a", 1.0],
            ["c", 3.0],
          ])
        end
      end
      context "removing items past the end of the set" do
        it "returns 0 and removes nothing from the set" do
          expect(
            redis.zremrangebyrank("foo", 10, 20)
          ).to eql(0)

          expect(
            redis.zrange("foo",0,2, with_scores: true)
          ).to eql([
            ["a", 1.0],
            ["b", 2.0],
            ["c", 3.0],
          ])
        end
      end
      context "removing items before the start of the set" do
        it "returns 0 and removes nothing from the set" do
          expect(
            redis.zremrangebyrank("foo", -20, -10)
          ).to eql(0)

          expect(
            redis.zrange("foo",0,2, with_scores: true)
          ).to eql([
            ["a", 1.0],
            ["b", 2.0],
            ["c", 3.0],
          ])
        end
      end
    end
    context "when the set has 3 items with equal scores" do
      before do
        redis.zadd("foo","1","a")
        redis.zadd("foo","1","b")
        redis.zadd("foo","1","c")
      end
      context "removing two of them" do
        it "returns 2 and removes the items from the set" do
          expect(
            redis.zremrangebyrank("foo", 0, 1)
          ).to eql(2)

          expect(
            redis.zrange("foo",0,2, with_scores: true)
          ).to eql([
            ["c", 1.0]
          ])
        end
      end
    end
  end
  context "zremrangebyscore" do
    context "when the set doesn't exist" do
      it "returns 0" do
        expect(
          redis.zremrangebyscore("foo", 0, 1)
        ).to eql(0)
      end
    end
    context "when the set has 3 items" do
      before do
        redis.zadd("foo","2","b")
        redis.zadd("foo","1","a")
        redis.zadd("foo","3","c")
      end
      context "removing two of them with inclusive syntax" do
        it "returns 2 and removes the items from the set" do
          expect(
            redis.zremrangebyscore("foo", 1, 2)
          ).to eql(2)

          expect(
            redis.zrange("foo",0,2, with_scores: true)
          ).to eql([
            ["c", 3.0]
          ])
        end
      end
      context "removing one of them with exclusive syntax" do
        it "returns 1 and removes the item from the set" do
          expect(
            redis.zremrangebyscore("foo", "1", "(2")
          ).to eql(1)

          expect(
            redis.zrange("foo",0,2, with_scores: true)
          ).to eql([
            ["b", 2.0],
            ["c", 3.0],
          ])
        end
      end
      context "removing one of them with negative infinite syntax" do
        it "returns 1 and removes the item from the set" do
          expect(
            redis.zremrangebyscore("foo", "-inf", "1")
          ).to eql(1)

          expect(
            redis.zrange("foo",0,2, with_scores: true)
          ).to eql([
            ["b", 2.0],
            ["c", 3.0],
          ])
        end
      end
      context "removing one of them with positive infinite syntax" do
        it "returns 1 and removes the item from the set" do
          expect(
            redis.zremrangebyscore("foo", "3", "+inf")
          ).to eql(1)

          expect(
            redis.zrange("foo",0,2, with_scores: true)
          ).to eql([
            ["a", 1.0],
            ["b", 2.0],
          ])
        end
      end
    end
    context "when the set has 3 items with float values" do
      before do
        redis.zadd("foo","1.2","b")
        redis.zadd("foo","1.1","a")
        redis.zadd("foo","1.3","c")
      end
      context "removing two of them with inclusive syntax" do
        it "returns 2 and removes the items from the set" do
          expect(
            redis.zremrangebyscore("foo", "1.1", "1.2")
          ).to eql(2)

          expect(
            redis.zrange("foo",0,2, with_scores: true)
          ).to eql([
            ["c", 1.3]
          ])
        end
      end
    end
  end
end
