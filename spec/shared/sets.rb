# coding: utf-8

RSpec.shared_examples "sets" do
  context "sadd" do
    context "when the set doesn't exist" do
      it "creates the set and returns 1" do
        expect(
          redis.sadd("foo","a")
        ).to eql(true)
      end
      it "adds the item to the set"
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
        it "adds the item to the set"
      end

      context "adding two new items" do
        it "returns the number of items added" do
          expect(
            redis.sadd("foo",["b","c"])
          ).to eql(2)
        end
        it "adds the items to the set"
      end

      context "adding one new and one existing item to the set" do
        it "returns the number of items added" do
          expect(
            redis.sadd("foo",["a","b"])
          ).to eql(1)
        end
        it "adds the new item to the set"
      end

      context "adding an existing item to the set" do
        it "returns the number of items added" do
          expect(
            redis.sadd("foo","a")
          ).to eql(false)
        end
        it "doesn't modify the set"
      end
    end
  end
end
