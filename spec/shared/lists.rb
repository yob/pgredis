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

end
