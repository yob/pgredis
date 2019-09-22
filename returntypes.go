package pgredis

import (
	"io"

	"github.com/secmask/go-redisproto"
)

type pgRedisValue interface {
	writeTo(io.Writer) error
	raw() interface{}
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

func (num *pgRedisInt) raw() interface{} {
	return num.value
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

func (str *pgRedisString) raw() interface{} {
	return str.value
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

func (err *pgRedisError) raw() interface{} {
	return err.value
}

type pgRedisNil struct{}

// TODO should this return a pgRedisError or pgRedisValue?
func newPgRedisNil() pgRedisValue {
	return &pgRedisNil{}
}

func (empty *pgRedisNil) writeTo(target io.Writer) error {
	protocolWriter := redisproto.NewWriter(target)
	return protocolWriter.WriteBulk(nil)
}

func (empty *pgRedisNil) raw() interface{} {
	return nil
}

type pgRedisNilArray struct{}

// TODO should this return a pgRedisError or pgRedisValue?
func newPgRedisNilArray() pgRedisValue {
	return &pgRedisNilArray{}
}

func (empty *pgRedisNilArray) writeTo(target io.Writer) error {
	protocolWriter := redisproto.NewWriter(target)
	return protocolWriter.WriteObjectsSlice(nil)
}

func (empty *pgRedisNilArray) raw() interface{} {
	return nil
}

type pgRedisArray struct {
	values []pgRedisValue
}

// TODO should this return a pgRedisError or pgRedisValue?
func newPgRedisArray(values []pgRedisValue) pgRedisValue {
	return pgRedisArray{
		values: values,
	}
}

func newPgRedisArrayOfStrings(values []string) pgRedisValue {
	newValues := make([]pgRedisValue, len(values))
	for idx, value := range values {
		newValues[idx] = newPgRedisString(value)
	}

	return pgRedisArray{
		values: newValues,
	}
}

func (arr pgRedisArray) writeTo(target io.Writer) error {
	rawValues := make([]interface{}, len(arr.values))
	for idx, value := range arr.values {
		if value == nil {
			rawValues[idx] = nil
		} else {
			rawValues[idx] = value.raw()
		}
	}
	protocolWriter := redisproto.NewWriter(target)
	return protocolWriter.WriteObjects(rawValues...)
}

func (arr pgRedisArray) raw() interface{} {
	return arr.values
}

type pgRedisScanReponse struct {
	cursor string
	values []string
}

func newPgRedisScanResponse(cursor string, values []string) pgRedisValue {
	return pgRedisScanReponse{
		cursor: cursor,
		values: values,
	}
}

func (resp pgRedisScanReponse) writeTo(target io.Writer) error {
	star := []byte{'*'}
	newLine := []byte{'\r', '\n'}
	protocolWriter := redisproto.NewWriter(target)
	protocolWriter.Write(star)        // start an array
	protocolWriter.Write([]byte("2")) // the array has 2 elements
	protocolWriter.Write(newLine)

	// first element is the cursor
	protocolWriter.WriteBulkString(resp.cursor)

	// second element is a nested array
	return protocolWriter.WriteBulkStrings(resp.values)
}

func (resp pgRedisScanReponse) raw() interface{} {
	return resp.values
}
