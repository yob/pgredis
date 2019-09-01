package repositories

import (
	"database/sql"
	"fmt"
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

type StringRepository struct {
	db       *sql.DB
}

func NewStringRepository(db *sql.DB) *StringRepository {
	return &StringRepository{
		db: db,
	}
}

func (repo *StringRepository) Get(key []byte) (bool, RedisString, error) {
	result := RedisString{}
	var expiresAt pq.NullTime

	sqlStat := "SELECT key, value, expires_at FROM redisdata WHERE key = $1 AND (expires_at > now() OR expires_at IS NULL)"
	row := repo.db.QueryRow(sqlStat, key)

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

func (repo *StringRepository) InsertOrUpdate(key []byte, value []byte, expiry_millis int) (err error) {
	if expiry_millis == 0 {
		sqlStat := "INSERT INTO redisdata(key, value, expires_at) VALUES ($1, $2, NULL) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, expires_at = NULL"
		_, err = repo.db.Exec(sqlStat, key, value)
	} else {
		sqlStat := "INSERT INTO redisdata(key, value, expires_at) VALUES ($1, $2, now() + cast($3 as interval)) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at"
		interval := fmt.Sprintf("%d milliseconds", expiry_millis)
		_, err = repo.db.Exec(sqlStat, key, value, interval)
	}
	if err != nil {
		return err
	}
	return nil
}

func (repo *StringRepository) InsertOrSkip(key []byte, value []byte, expiry_millis int) (inserted bool, err error) {
	// TODO delete any expired rows in the db with this key
	var res sql.Result
	if expiry_millis == 0 {
		sqlStat := "INSERT INTO redisdata(key, value, expires_at) VALUES ($1, $2, NULL) ON CONFLICT (key) DO NOTHING"
		res, err = repo.db.Exec(sqlStat, key, value)
		count, _ := res.RowsAffected()
		inserted = count > 0
	} else {
		sqlStat := "INSERT INTO redisdata(key, value, expires_at) VALUES ($1, $2, now() + cast($3 as interval)) ON CONFLICT DO NOTHING"
		interval := fmt.Sprintf("%d milliseconds", expiry_millis)
		res, err = repo.db.Exec(sqlStat, key, value, interval)
		count, _ := res.RowsAffected()
		inserted = count > 0
	}
	if err != nil {
		return inserted, err
	}
	return inserted, nil
}

func (repo *StringRepository) UpdateOrSkip(key []byte, value []byte, expiry_millis int) (updated bool, err error) {
	// TODO delete any expired rows in the db with this key
	var res sql.Result
	if expiry_millis == 0 {
		sqlStat := "UPDATE redisdata SET value=$2, expires_at=NULL WHERE key=$1 AND (expires_at IS NULL OR expires_at < now())"
		res, err = repo.db.Exec(sqlStat, key, value)
		count, _ := res.RowsAffected()
		updated = count > 0
	} else {
		sqlStat := "UPDATE redisdata SET value=$2, expires_at=now() + cast($3 as interval) WHERE key=$1 AND (expires_at IS NULL OR expires_at < now())"
		interval := fmt.Sprintf("%d milliseconds", expiry_millis)
		res, err = repo.db.Exec(sqlStat, key, value, interval)
		count, _ := res.RowsAffected()
		updated = count > 0
	}
	if err != nil {
		return updated, err
	}
	return false, nil
}

func (repo *StringRepository) InsertOrAppend(key []byte, value []byte) ([]byte, error) {
	// TODO delete any expired rows in the db with this key
	var finalValue []byte

	sqlStat := "INSERT INTO redisdata(key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = redisdata.value || EXCLUDED.value RETURNING value"
	err := repo.db.QueryRow(sqlStat, key, value).Scan(&finalValue)
	if err != nil {
		return nil, err
	}
	return finalValue, nil
}

func (repo *StringRepository) Incr(key []byte, by int) ([]byte, error) {
	var finalValue []byte

	sqlStat := "INSERT INTO redisdata(key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = CASE WHEN redisdata.expires_at < now() THEN $3 ELSE ((cast(encode(redisdata.value,'escape') as integer)+$4)::text)::bytea END , expires_at = NULL RETURNING value"
	err := repo.db.QueryRow(sqlStat, key, by, by, by).Scan(&finalValue)
	if err != nil {
		return nil, err
	}
	return finalValue, nil
}

func (repo *StringRepository) IncrDecimal(key []byte, by float64) ([]byte, error) {
	var finalValue []byte

	sqlStat := "INSERT INTO redisdata(key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = CASE WHEN redisdata.expires_at < now() THEN $3 ELSE ((cast(encode(redisdata.value,'escape') as decimal)+$4)::text)::bytea END, expires_at = NULL RETURNING value"
	err := repo.db.QueryRow(sqlStat, key, by, by, by).Scan(&finalValue)
	if err != nil {
		return nil, err
	}
	return finalValue, nil
}

func (repo *StringRepository) FlushAll() error {
	sqlStat := "DELETE FROM redisdata"
	_, err := repo.db.Exec(sqlStat)
	if err != nil {
		return err
	}
	return nil
}
