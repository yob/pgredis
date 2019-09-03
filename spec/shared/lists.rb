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
      it "returns the list size"
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
        it "appends the new value to the end of the list"
      end

      context "pushing multiple items" do
        it "returns the new size" do
          expect(
            redis.rpush("foo", ["baz", "boo"])
          ).to eql(3)
        end
        it "appends the new value to the end of the list"
      end
    end

  end

end
