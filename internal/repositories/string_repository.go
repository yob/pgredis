package repositories

import (
	"database/sql"
	"fmt"
	"sort"
	"time"

	"github.com/lib/pq"
)

type RedisString struct {
	Key        []byte
	Value      []byte
	Expires_at time.Time
}

func (str *RedisString) TTLInSeconds() int64 {
	if str.Expires_at.IsZero() {
		return 0
	} else {
		diff := str.Expires_at.Sub(time.Now()).Seconds()
		return int64(diff)
	}
}

func (str *RedisString) WillExpire() bool {
	if str.Expires_at.IsZero() {
		return false
	} else {
		return true
	}
}

type StringRepository struct{}

func NewStringRepository() *StringRepository {
	return &StringRepository{}
}

func (repo *StringRepository) Get(tx *sql.Tx, key []byte) (bool, RedisString, error) {
	result := RedisString{}
	var expiresAt pq.NullTime

	sqlStat := "SELECT key, value, expires_at FROM redisdata WHERE key = $1 AND (expires_at > now() OR expires_at IS NULL)"
	row := tx.QueryRow(sqlStat, key)

	switch err := row.Scan(&result.Key, &result.Value, &expiresAt); err {
	case sql.ErrNoRows:
		return false, result, nil
	case nil:
		if expiresAt.Valid {
			result.Expires_at = expiresAt.Time
		}
		return true, result, nil
	default:
		return false, result, err
	}
}

func (repo *StringRepository) InsertOrUpdate(tx *sql.Tx, key []byte, value []byte, expiry_millis int) (err error) {
	items := make(map[string]string)
	items[string(key)] = string(value)
	return repo.InsertOrUpdateMultiple(tx, items, expiry_millis)
}

func (repo *StringRepository) InsertOrUpdateMultiple(tx *sql.Tx, items map[string]string, expiry_millis int) (err error) {
	keys := make([]string, len(items))

	// take an exclusive lock for each key, in sorted order to avoid deadlocks
	for key, _ := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		sqlStat := "SELECT pg_advisory_xact_lock(hashtext($1))"
		_, err = tx.Exec(sqlStat, key)
		if err != nil {
			return err
		}
	}

	// with deadlock-avoiding sorted locks in plac, now it's safe to modify
	// values in user-provided order
	for key, value := range items {
		if expiry_millis == 0 {
			sqlStat := "INSERT INTO redisdata(key, type, value, expires_at) VALUES ($1, 'string', $2, NULL) ON CONFLICT (key) DO UPDATE SET type='string', value = EXCLUDED.value, expires_at = NULL"
			_, err = tx.Exec(sqlStat, key, value)
		} else {
			sqlStat := "INSERT INTO redisdata(key, type, value, expires_at) VALUES ($1, 'string', $2, now() + cast($3 as interval)) ON CONFLICT (key) DO UPDATE SET type='string', value = EXCLUDED.value, expires_at = EXCLUDED.expires_at"
			interval := fmt.Sprintf("%d milliseconds", expiry_millis)
			_, err = tx.Exec(sqlStat, key, value, interval)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (repo *StringRepository) InsertOrSkip(tx *sql.Tx, key []byte, value []byte, expiry_millis int) (inserted bool, err error) {

	// take an exclusive lock for this key
	sqlStat := "SELECT pg_advisory_xact_lock(hashtext($1))"
	_, err = tx.Exec(sqlStat, key)
	if err != nil {
		return false, err
	}

	// delete any expired rows in the db with this key
	sqlStat = "DELETE FROM redisdata WHERE key=$1 AND expires_at < now()"
	_, err = tx.Exec(sqlStat, key)
	if err != nil {
		return false, err
	}

	var res sql.Result
	if expiry_millis == 0 {
		sqlStat := "INSERT INTO redisdata(key, type, value, expires_at) VALUES ($1, 'string', $2, NULL) ON CONFLICT (key) DO NOTHING"
		res, err = tx.Exec(sqlStat, key, value)
		count, _ := res.RowsAffected()
		inserted = count > 0
	} else {
		sqlStat := "INSERT INTO redisdata(key, type, value, expires_at) VALUES ($1, 'string', $2, now() + cast($3 as interval)) ON CONFLICT DO NOTHING"
		interval := fmt.Sprintf("%d milliseconds", expiry_millis)
		res, err = tx.Exec(sqlStat, key, value, interval)
		count, _ := res.RowsAffected()
		inserted = count > 0
	}
	if err != nil {
		return false, err
	}

	return inserted, nil
}

func (repo *StringRepository) UpdateOrSkip(tx *sql.Tx, key []byte, value []byte, expiry_millis int) (updated bool, err error) {

	// take an exclusive lock for this key
	sqlStat := "SELECT pg_advisory_xact_lock(hashtext($1))"
	_, err = tx.Exec(sqlStat, key)
	if err != nil {
		return false, err
	}

	// delete any expired rows in the db with this key
	sqlStat = "DELETE FROM redisdata WHERE key=$1 AND expires_at < now()"
	_, err = tx.Exec(sqlStat, key)
	if err != nil {
		return false, err
	}

	var res sql.Result
	if expiry_millis == 0 {
		sqlStat := "UPDATE redisdata SET type='string', value=$2, expires_at=NULL WHERE key=$1"
		res, err = tx.Exec(sqlStat, key, value)
		count, _ := res.RowsAffected()
		updated = count > 0
	} else {
		sqlStat := "UPDATE redisdata SET type='string', value=$2, expires_at=now() + cast($3 as interval) WHERE key=$1"
		interval := fmt.Sprintf("%d milliseconds", expiry_millis)
		res, err = tx.Exec(sqlStat, key, value, interval)
		count, _ := res.RowsAffected()
		updated = count > 0
	}
	if err != nil {
		return false, err
	}

	return updated, nil
}

func (repo *StringRepository) InsertOrAppend(tx *sql.Tx, key []byte, value []byte) ([]byte, error) {
	var finalValue []byte

	// take an exclusive lock for this key
	sqlStat := "SELECT pg_advisory_xact_lock(hashtext($1))"
	_, err := tx.Exec(sqlStat, key)
	if err != nil {
		return finalValue, err
	}

	// delete any expired rows in the db with this key
	sqlStat = "DELETE FROM redisdata WHERE key=$1 AND expires_at < now()"
	_, err = tx.Exec(sqlStat, key)
	if err != nil {
		return nil, err
	}

	sqlStat = "INSERT INTO redisdata(key, type, value) VALUES ($1, 'string', $2) ON CONFLICT (key) DO UPDATE SET type = 'string', value = redisdata.value || EXCLUDED.value RETURNING value"
	err = tx.QueryRow(sqlStat, key, value).Scan(&finalValue)
	if err != nil {
		return nil, err
	}

	return finalValue, nil
}

func (repo *StringRepository) Incr(tx *sql.Tx, key []byte, by int) ([]byte, error) {
	var finalValue []byte

	// take an exclusive lock for this key
	sqlStat := "SELECT pg_advisory_xact_lock(hashtext($1))"
	_, err := tx.Exec(sqlStat, key)
	if err != nil {
		return finalValue, err
	}

	sqlStat = "INSERT INTO redisdata(key, type, value) VALUES ($1, 'string', $2) ON CONFLICT (key) DO UPDATE SET type='string', value = CASE WHEN redisdata.expires_at < now() THEN $3 ELSE ((cast(encode(redisdata.value,'escape') as integer)+$4)::text)::bytea END , expires_at = NULL RETURNING value"
	err = tx.QueryRow(sqlStat, key, by, by, by).Scan(&finalValue)
	if err != nil {
		return nil, err
	}
	return finalValue, nil
}

func (repo *StringRepository) IncrDecimal(tx *sql.Tx, key []byte, by float64) ([]byte, error) {
	var finalValue []byte

	// take an exclusive lock for this key
	sqlStat := "SELECT pg_advisory_xact_lock(hashtext($1))"
	_, err := tx.Exec(sqlStat, key)
	if err != nil {
		return finalValue, err
	}

	sqlStat = "INSERT INTO redisdata(key, type, value) VALUES ($1, 'string', $2) ON CONFLICT (key) DO UPDATE SET type='string', value = CASE WHEN redisdata.expires_at < now() THEN $3 ELSE ((cast(encode(redisdata.value,'escape') as decimal)+$4)::text)::bytea END, expires_at = NULL RETURNING value"
	err = tx.QueryRow(sqlStat, key, by, by, by).Scan(&finalValue)
	if err != nil {
		return nil, err
	}
	return finalValue, nil
}
