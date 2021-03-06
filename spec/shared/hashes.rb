# coding: utf-8

RSpec.shared_examples "hashes" do
  context "hget" do
    context "when the hash doesn't exist" do
      it "returns nil" do
        expect(
          redis.hget("foo","bar")
        ).to eql(nil)
      end
    end
    context "when the hash exists with a single field" do
      before do
        redis.hset("foo", "bar","1")
      end
      context "requesting that field" do
        it "returns the value" do
          expect(
            redis.hget("foo", "bar")
          ).to eql("1")
        end
      end

      context "requesting a field that doesn't exist" do
        it "returns nil" do
          expect(
            redis.hget("foo", "aaa")
          ).to eql(nil)
        end
      end
    end
    context "when the hash exists with a single field but it's expired" do
      before do
        redis.hset("foo", "bar", "10")
        redis.expire("foo", 1) # TODO change this to pexpire
        sleep(1.1)
      end
      it "returns nil" do
        expect(
          redis.hget("foo", "bar")
        ).to eql(nil)
      end
    end
  end

  context "hgetall" do
    context "when the hash doesn't exist" do
      it "returns an empty array" do
        expect(
          redis.hgetall("foo")
        ).to eql({})
      end
    end
    context "when the hash exists with two fields" do
      before do
        redis.hset("foo", "bar","1")
        redis.hset("foo", "baz","2")
      end
      context "requesting all values" do
        it "returns the fields and values in an array" do
          expect(
            redis.hgetall("foo")
          ).to eql({"bar" => "1", "baz" => "2"})
        end
      end
    end
    context "when the hash exists with a single field but it's expired" do
      before do
        redis.hset("foo", "bar", "1")
        redis.expire("foo", 1) # TODO change this to pexpire
        sleep(1.1)
      end
      it "returns an empty array" do
        expect(
          redis.hgetall("foo")
        ).to eql({})
      end
    end
  end

  context "hmget" do
    context "when the hash doesn't exist" do
      it "returns an array with nil" do
        expect(
          redis.hmget("foo","bar")
        ).to eql([nil])
      end
    end
    context "when the hash exists with two fields field" do
      before do
        redis.hset("foo", "bar","1")
        redis.hset("foo", "baz","2")
      end
      context "requesting both fields" do
        it "returns an array with the values" do
          expect(
            redis.hmget("foo", "bar", "baz")
          ).to eql(["1","2"])
        end
      end

      context "requesting a field that doesn't exist" do
        it "returns an array with the values or nil" do
          expect(
            redis.hmget("foo", "aaa", "bar")
          ).to eql([nil,"1"])
        end
      end
    end
    context "when the hash exists with a single field but it's expired" do
      before do
        redis.hset("foo", "bar", "10")
        redis.expire("foo", 1) # TODO change this to pexpire
        sleep(1.1)
      end
      it "returns an array with nil" do
        expect(
          redis.hmget("foo", "bar")
        ).to eql([nil])
      end
    end
  end

  context "hset" do
    context "when the hash doesn't exist" do
      it "returns 0" do
        expect(
          redis.hset("foo","bar","1")
        ).to eql(true)
      end
      it "creates the hash on demand" do
        redis.hset("foo","bar","1")
        expect(
          redis.hgetall("foo")
        ).to eql({"bar" => "1"})
      end
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
        it "adds the field to the hash" do
          redis.hset("foo", "baz", "2")
          expect(
            redis.hgetall("foo")
          ).to eql({"bar" => "1", "baz" => "2"})
        end
      end

      context "updating an existing field" do
        it "returns 0" do
          expect(
            redis.hset("foo", "bar", "2")
          ).to eql(false)
        end
        it "updates the field" do
          redis.hset("foo", "bar", "2")
          expect(
            redis.hgetall("foo")
          ).to eql({"bar" => "2"})
        end
      end
    end

    context "hmset" do
      context "when the hash doesn't exist" do
        it "returns 'OK'" do
          expect(
            redis.hmset("foo","bar","1")
          ).to eql("OK")
        end
        it "creates the hash on demand" do
          redis.hmset("foo","bar","1")
          expect(
            redis.hgetall("foo")
          ).to eql({"bar" => "1"})
        end
      end
      context "when the hash exists with a single field" do
        before do
          redis.hset("foo", "bar","1")
        end
        context "adding two new fields" do
          it "returns OK" do
            expect(
              redis.hmset("foo", "aaa", "2", "bbb", "3")
            ).to eql("OK")
          end
          it "adds the field to the hash" do
            redis.hmset("foo", "aaa", "2", "bbb", "3")
            expect(
              redis.hgetall("foo")
            ).to eql({"bar" => "1", "aaa" => "2", "bbb" => "3"})
          end
        end

        context "updating an existing field" do
          it "returns OK" do
            expect(
              redis.hmset("foo", "bar", "2")
            ).to eql("OK")
          end
          it "updates the field" do
            redis.hmset("foo", "bar", "2")
            expect(
              redis.hgetall("foo")
            ).to eql({"bar" => "2"})
          end
        end
      end
    end
  end
end
