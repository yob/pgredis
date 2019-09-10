# coding: utf-8

RSpec.shared_examples "hashes" do
  context "hset" do
    context "when the hash doesn't exist" do
      it "returns 0" do
        expect(
          redis.hset("foo","bar","1")
        ).to eql(true)
      end
      it "creates the hash on demand"
    end
    context "when the hash exists with a single field" do
      before do
        redis.hset("foo", "bar","1")
      end
      context "adding a new field" do
        it "returns 1" do
          expect(
            redis.hset("foo", "baz", "2")
          ).to eql(true)
        end
        it "adds the field to the hash"
      end

      context "updating an existing field" do
        it "returns 0" do
          expect(
            redis.hset("foo", "bar", "2")
          ).to eql(false)
        end
        it "updates the field"
      end
    end

  end
end
