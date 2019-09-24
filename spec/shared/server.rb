# coding: utf-8

RSpec.shared_examples "server" do
  context "info" do
    context "with no arg" do
      it "returns data separated by newlines (converted to a Hash by redis-rb)" do
        result = redis.info
        expect(result).to be_a(Hash)
        expect(result.keys).to include("redis_version")
      end
    end
    context "with an arg" do
      it "returns just the info for that section"
    end

  end
end
