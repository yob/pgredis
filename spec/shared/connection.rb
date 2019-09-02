# coding: utf-8

RSpec.shared_examples "connection" do
  context "ping" do
    context "with no arg" do
      it "returns PONG" do
        expect(
          redis.ping
        ).to eql("PONG")
      end
    end
    context "with an arg" do
      it "returns the arg" do
        expect(
          redis.ping("Hi")
        ).to eql("Hi")
      end
    end

  end
end

