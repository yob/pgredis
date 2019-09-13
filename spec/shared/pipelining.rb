# coding: utf-8

RSpec.shared_examples "pipelining" do
  context "when the key exists" do
    before do
      redis.incr("foo")
    end
    it "returns applies multiple changes and makes the values available" do
      res_one = nil
      res_two = nil
      redis.pipelined do
        res_one = redis.set("bar", "baz")
        res_two = redis.incr("foo")
      end

      expect( res_one.value ).to eql("OK")
      expect( res_two.value ).to eql(2)

      expect( redis.get("foo") ).to eql("2")
      expect( redis.get("bar") ).to eql("baz")
    end
  end
end
