# coding: utf-8

RSpec.shared_examples "sorted sets" do
  context "zadd" do
    context "when the set doesn't exist" do
      it "creates the set and returns 1" do
        expect(
          redis.zadd("foo","1.1", "a")
        ).to eql(true)
      end
      it "adds the item to the set"
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
        it "adds the item to the set"
      end
      context "adding a multiple new items" do
        it "returns the number of items added" do
          expect(
            redis.zadd("foo",[["1.0","b"],["2.0","c"]])
          ).to eql(2)
        end
        it "adds both items to the set"
      end
      context "adding an existing item with an identical score" do
        it "returns the number of items added" do
          expect(
            redis.zadd("foo","1.1","a")
          ).to eql(false)
        end
        it "does not modify the set"
      end
      context "adding an existing item with a newscore" do
        it "returns the number of items added, exlcuding items that only had the score updated" do
          expect(
            redis.zadd("foo","1.2","a")
          ).to eql(false)
        end
        it "updates the score of the existing item"
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
          pending "returns the number of items added or updated" do
            expect(
              redis.zadd("foo","1.0","b", ch: true)
            ).to eql(true)
          end
        end
        context "adding an existing item with an identical score" do
          pending "returns the number of items added or updated" do
            expect(
              redis.zadd("foo","1.1","a", ch: true)
            ).to eql(false)
          end
        end
        context "adding an existing item with a newscore" do
          pending "returns the number of items added, including items that only had the score updated" do
            expect(
              redis.zadd("foo","1.2","a", ch: true)
            ).to eql(true)
          end
        end
      end
      context "with INCR option" do
        context "adding a new item" do
          pending "returns the score of the item added" do
            expect(
              redis.zadd("foo","1.0","b", incr: true)
            ).to eql(1.0)
          end
        end
        context "incrementing an existing item" do
          pending "returns the new score of the item" do
            expect(
              redis.zadd("foo","1.0","a", incr: true)
            ).to eql(2.1)
          end
        end
      end
    end
  end
end
