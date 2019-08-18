# coding: utf-8

RSpec.shared_examples "strings" do
  context "setting a short string" do
    it "returns OK" do
      expect(
        redis.set("foo", "s1")
      ).to eql("OK")
    end
  end
  context "setting nil" do
    it "returns OK" do
      expect(
        redis.set("foo",nil)
      ).to eql("OK")
    end
  end

  context  "with an existing key and short value" do
    before do
      redis.set("foo", "s1")
    end

    it "get returns the value" do
      expect(redis.get("foo")).to eql("s1")
    end
  end

  context  "with an existing key and value that has trailing whitespace" do
    before do
      redis.set("foo", "s1\n")
    end

    it "returns the value" do
      expect(redis.get("foo")).to eql("s1\n")
    end
  end

  context "when the key doesn't exist" do
    it "returns nil" do
      expect(redis.get("foo")).to eql(nil)
    end
  end

  context "with non ASCII values" do
    pending "returns all values unmodified" do
      (0..255).each do |i|
        str = "#{i.chr}---#{i.chr}"
        redis.set("foo", str)
        expect(redis.get("foo")).to eql(str)
      end
    end
  end

  context "set with ex" do
    it "expires the key after the requested seconds" do
      redis.set("foo", "bar", ex: 2)
      expect(redis.get("foo")).to eql("bar")
      sleep(2)
      expect(redis.get("foo")).to eql(nil)
    end
    it "records a ttl in seconds on the key" do
      redis.set("foo", "bar", ex: 2)
      expect(redis.ttl("foo")).to be_between(0, 2)
    end
  end

  context "set with px" do
    it "expires the key after the requested milliseconds" do
      redis.set("foo", "bar", px: 2000)
      expect(redis.get("foo")).to eql("bar")
      sleep(2)
      expect(redis.get("foo")).to eql(nil)
    end
    it "records a ttl in milliseconds on the key" do
      redis.set("foo", "bar", px: 2000)
      expect(redis.ttl("foo")).to be_between(0, 2)
    end
  end

  context "set with nx" do

    context "when the key already exist" do
      it "does not set the key" do
        redis.set("foo", "bar")
        redis.set("foo", "baz", nx: true)
        expect(redis.get("foo")).to eql("bar")
      end
    end

    context "when the key does not already exist" do
      it "sets the key" do
        redis.set("foo", "baz", nx: true)
        expect(redis.get("foo")).to eql("baz")
      end
    end
  end

  context "set with xx" do

    context "when the key already exists" do
      it "sets the key" do
        redis.set("foo", "bar")
        redis.set("foo", "baz", xx: true)
        expect(redis.get("foo")).to eql("baz")
      end
    end

    context "when the key does not already exists" do
      it "does not set the key" do
        expect(
          redis.set("foo", "baz", xx: true)
        ).to eql(false)
      end
    end
  end

  context "setex" do
    it "expires the key after the requested seconds" do
      redis.setex("foo", 2, "bar")
      expect(redis.get("foo")).to eql("bar")
      sleep(2)
      expect(redis.get("foo")).to eql(nil)
    end

    it "records a ttl in seconds on the key" do
      redis.setex("foo", 2, "bar")
      expect(redis.ttl("foo")).to be_between(0, 2)
    end
  end

  context "psetex" do
    it "expires the key after the requested milliseconds" do
      redis.psetex("foo", 2000, "bar")
      expect(redis.get("foo")).to eql("bar")
      sleep(2)
      expect(redis.get("foo")).to eql(nil)
    end
    it "records a ttl in milliseconds on the key" do
      redis.psetex("foo", 2000, "bar")
      expect(redis.ttl("foo")).to be_between(0, 2)
    end
  end

  context "setnx" do

    context "when the key already exists" do
      it "does not set the key" do
        redis.set("foo", "bar")
        redis.setnx("foo", "baz")
        expect(redis.get("foo")).to eql("bar")
      end
    end

    context "when the key does not already exist" do
      it "sets the key" do
        redis.setnx("foo", "baz")
        expect(redis.get("foo")).to eql("baz")
      end
    end
  end

  context "getset" do
    context "when there's a previous value" do
      it "sets a new value and returns the previous value" do
        redis.set("foo", "bar")
        expect(redis.getset("foo", "baz")).to eql("bar")
        expect(redis.get("foo")).to eql("baz")
      end
    end
    context "when there's no previous value" do
      it "sets a new value and returns nil" do
        expect(redis.getset("foo", "baz")).to eql(nil)
        expect(redis.get("foo")).to eql("baz")
      end
    end
  end

  context "incr" do
    pending "increments a counter each time" do
      expect(redis.incr("foo")).to eql(1)
      expect(redis.incr("foo")).to eql(2)
      expect(redis.incr("foo")).to eql(3)
    end
  end

  context "incrby" do
    pending "increments a counter each time" do
      expect(redis.incrby("foo", 1)).to eql(1)
      expect(redis.incrby("foo", 2)).to eql(3)
      expect(redis.incrby("foo", 3)).to eql(6)
    end
  end

  context "incrbyfloat" do
    pending "increments a counter each time" do
      expect(redis.incrbyfloat("foo", 1.23)).to eql(1.23)
      expect(redis.incrbyfloat("foo", 0.77)).to eql(2.0)
      expect(redis.incrbyfloat("foo", -0.1)).to eql(1.9)
    end
  end

  context "decr" do
    pending "decrements a counter each time" do
      redis.set("foo", 3)
      expect(redis.decr("foo")).to eql(2)
      expect(redis.decr("foo")).to eql(1)
      expect(redis.decr("foo")).to eql(0)
    end
  end

  context "decrby" do
    pending "decrements a counter each time" do
      redis.set("foo", 6)
      expect(redis.decrby("foo", 3)).to eql(3)
      expect(redis.decrby("foo", 2)).to eql(1)
      expect(redis.decrby("foo", 1)).to eql(0)
    end
  end

  context "append" do
    context "when the key already exists" do
      it "appends to the end of the existing value" do
        redis.set("foo", "s")
        redis.append("foo", "1")
        expect(redis.get("foo")).to eql("s1")
      end
    end
    context "when the key doesn't exist" do
      it "starts a new value" do
        redis.append("foo", "1")
        expect(redis.get("foo")).to eql("1")
      end
    end
  end

  context "getbit" do
    pending "returns the bit at the requested position" do
      redis.set("foo", "a")

      expect(redis.getbit("foo", 1)).to eql(1)
      expect(redis.getbit("foo", 2)).to eql(1)
      expect(redis.getbit("foo", 3)).to eql(0)
      expect(redis.getbit("foo", 4)).to eql(0)
      expect(redis.getbit("foo", 5)).to eql(0)
      expect(redis.getbit("foo", 6)).to eql(0)
      expect(redis.getbit("foo", 7)).to eql(1)
    end
  end

  context "setbit" do
    pending "changes the bit at the requested position" do
      redis.set("foo", "a")

      redis.setbit("foo", 6, 1)

      expect(redis.get("foo")).to eql("c")
    end
  end

  context "bitcount" do
    context "when the key exists" do
      pending "returns the number of set bits in the value" do
        redis.set("foo", "abcde")

        expect(redis.bitcount("foo")).to eql(17)
        expect(redis.bitcount("foo", 1, 3)).to eql(10) # key, start, end
        expect(redis.bitcount("foo", 0, -1)).to eql(17) # key, start, end
      end
    end
    context "when the key doesn't exists" do
      pending "returns 0" do
        expect(redis.bitcount("foo")).to eql(0)
      end
    end
  end

  context "getrange" do
    it "returns the requested substring" do
      redis.set("foo", "abcde")

      expect(redis.getrange("foo", 1, 3)).to eql("bcd")
      expect(redis.getrange("foo", 0, -1)).to eql("abcde")
      expect(redis.getrange("foo", 1, 100)).to eql("bcde")
    end
  end

  context "strlen" do
    context "when the key exists" do
      it "returns the length of the value" do
        redis.set("foo", "abcde")

        expect(redis.strlen("foo")).to eql(5)
      end
    end
    context "when the key doesn't exists" do
      it "returns 0" do
        expect(redis.strlen("foo")).to eql(0)
      end
    end
  end

  context "bitfield" do
    it "does stuff"
  end

  context "mget" do
    pending "returns an array of the requested values" do
      redis.set("foo", "s1")
      redis.set("bar", "s2")

      expect(redis.mget("foo", "bar")).to eql(["s1", "s2"])
      expect(redis.mget("foo", "bar", "baz")).to eql(["s1", "s2", nil])
    end
  end

  context "mset" do
    it "does stuff"
  end

  context "msetnx" do
    it "does stuff"
  end

  context "bitop" do
    it "does stuff"
  end
end
