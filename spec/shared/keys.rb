# coding: utf-8

RSpec.shared_examples "keys" do
  context "del" do
    context "when the key exists" do
      before do
        redis.set("foo", 1)
        redis.set("bar", 2)
      end
      context "deleting a single key" do
        it "returns 1" do
          expect(
            redis.del("foo")
          ).to eql(1)
        end
      end
      context "deleting two keys" do
        it "returns 2" do
          expect(
            redis.del("foo","bar")
          ).to eql(2)
        end
      end

      it "sets an expiry on the key" do
        redis.expire("foo", 10)
        expect(redis.ttl("foo")).to be_between(0, 10)
      end
    end

    context "when the key already exists but it's expired" do
      before do
        redis.set("foo", 1, px: 1) # almost insta expire
        sleep(0.1)
      end

      it "returns 0" do
        expect(
          redis.del("foo")
        ).to eql(0)
      end
    end

    context "when the key does not exist" do
      it "returns 0" do
        expect(
          redis.del("foo")
        ).to eql(0)
      end
    end
  end

  context "exists" do
    before do
      redis.set("foo", 1)
      redis.set("bar", 2)
      redis.set("baz", 3, px: 1) # almost insta expire
      sleep(0.1)
    end
    context "when the key exists" do
      it "returns 1" do
        expect( redis.exists("foo") ).to eql(true)
      end
    end

    context "when multiple keys exists" do
      # redis 3.0+ supports multiple key arguments, but the ruby library we're using doesn't
      pending "returns 2" do
        expect( redis.exists("foo", "bar") ).to eql(2)
      end
    end

    context "when the keys exists it it's requestde multiple times" do
      # redis 3.0+ supports multiple key arguments, but the ruby library we're using doesn't
      pending "returns 2" do
        expect( redis.exists("foo", "foo") ).to eql(2)
      end
    end

    context "when the key doesn' exists" do
      it "returns 0" do
        expect( redis.exists("wow") ).to eql(false)
      end
    end

    context "when the key exists but it's expired" do
      it "returns 0" do
        expect( redis.exists("baz") ).to eql(false)
      end
    end
  end

  context "expire" do
    context "when the key exists" do
      before do
        redis.set("foo", 1)
        expect(redis.ttl("foo")).to eql(-1)
      end
      it "returns 1" do
        expect(
          redis.expire("foo", 10)
        ).to eql(true)
      end

      it "sets an expiry on the key" do
        redis.expire("foo", 10)
        expect(redis.ttl("foo")).to be_between(0, 10)
      end
    end

    context "when the key already exists but it's expired" do
      before do
        redis.set("foo", "bar", px: 1) # almost insta expire
        sleep(0.1)
      end

      it "returns 0 and doesn't resurrect the key" do
        expect(
          redis.expire("foo", 10)
        ).to eql(false)
        expect(
          redis.get("foo")
        ).to eql(nil)
      end
    end

    context "when the key does not exist" do
      it "returns 0" do
        expect(
          redis.expire("foo", 10)
        ).to eql(false)
      end
    end
  end

  context "ttl" do
    context "when the key exists with no expiry" do
      before do
        redis.set("foo", "bar")
      end
      it "returns -1" do
        expect(
          redis.ttl("foo")
        ).to eql(-1)
      end
    end

    context "when the key exists with an expiry" do
      before do
        redis.set("foo", "bar", ex: 10)
      end
      it "returns the time to live" do
        expect(redis.ttl("foo")).to be_between(0, 10)
      end
    end

    context "when the key already exists but it's expired" do
      before do
        redis.set("foo", "bar", px: 1) # almost insta expire
        sleep(0.1)
      end
      it "returns -2" do
        expect(
          redis.ttl("foo")
        ).to eql(-2)
      end
    end

    context "when the key does not exist" do
      it "returns -2" do
        expect(
          redis.ttl("foo")
        ).to eql(-2)
      end
    end

    context "when the key exists with an expiry but it's a list" do
      it "returns the time to live"
    end
  end
end
