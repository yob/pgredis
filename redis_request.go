package pgredis

import (
	"strings"

	"github.com/secmask/go-redisproto"
)

// We parse incomintg requests using the redisproto lib, which hands them to us as redisproto.Cmd
// structs. However, the properties of redisproto.Cmd aren't exported and the argv byte slice is
// reused for subsequent requests from the client, so they're not safe to keep in a queue.
//
// This type exists so we can copy redisproto.Cmd into our our struct and keep a queue in memory.
type redisRequest struct {
	argv [][]byte
	last bool
}

// Copy a redisproto.Cmd struct into a redisRequest
func newRequestFromRedisProto(from *redisproto.Command) redisRequest {
	last := from.IsLast()
	argv := make([][]byte, 0, 2)
	for i := 0; i < 100; i++ {
		next := from.Get(i)
		if next == nil {
			break
		}
		nextCopy := make([]byte, len(next))
		copy(nextCopy, next)
		argv = append(argv, nextCopy)
	}

	return redisRequest{argv: argv, last: last}
}

// Return the first argument of the request as a string. This is typically the redis command (GET,
// SET, etc)
func (c *redisRequest) CommandString() string {
	return strings.ToUpper(string(c.Get(0)))
}

// Fetch a space delimited part of the incoming request as a byte array. Exists for compatibility
// with redisproto.Cmd, and we may not keep it around forever.
func (c *redisRequest) Get(index int) []byte {
	if index >= 0 && index < len(c.argv) {
		return c.argv[index]
	} else {
		return nil
	}
}

// Return the number of arguments in the request. Exists for compatibility  with redisproto.Cmd,
// and we may not keep it around forever.
func (c *redisRequest) ArgCount() int {
	return len(c.argv)
}

// IsLast is true if this command is the last one in receive buffer, command handler should call writer.Flush()
// after write response, helpful in process pipeline command. Exists for compatibility  with redisproto.Cmd,
// and we may not keep it around forever.
func (c *redisRequest) IsLast() bool {
	return c.last
}
