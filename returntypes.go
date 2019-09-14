package pgredis

import (
	"io"

	"github.com/secmask/go-redisproto"
)


type pgRedisValue interface {
	writeTo(io.Writer) error
}

type pgRedisInt struct {
	value int64
}

// TODO should this return a pgRedisInt or pgRedisValue?
func newPgRedisInt(value int64) pgRedisValue {
	return &pgRedisInt{
		value: value,
	}
}

func (num *pgRedisInt) writeTo(target io.Writer) error {
	protocolWriter := redisproto.NewWriter(target)
	return protocolWriter.WriteInt(num.value)
}

type pgRedisString struct {
	value string
}

// TODO should this return a pgRedisString or pgRedisValue?
func newPgRedisString(value string) pgRedisValue {
	return &pgRedisString{
		value: value,
	}
}

func (str *pgRedisString) writeTo(target io.Writer) error {
	protocolWriter := redisproto.NewWriter(target)
	return protocolWriter.WriteBulkString(str.value)
}

type pgRedisError struct {
	value string
}

// TODO should this return a pgRedisError or pgRedisValue?
func newPgRedisError(value string) pgRedisValue {
	return &pgRedisError{
		value: value,
	}
}

func (err *pgRedisError) writeTo(target io.Writer) error {
	protocolWriter := redisproto.NewWriter(target)
	return protocolWriter.WriteError(err.value)
}

type pgRedisNil struct {}

// TODO should this return a pgRedisError or pgRedisValue?
func newPgRedisNil() pgRedisValue {
	return &pgRedisNil{}
}

func (empty *pgRedisNil) writeTo(target io.Writer) error {
	protocolWriter := redisproto.NewWriter(target)
	return protocolWriter.WriteBulk(nil)
}
