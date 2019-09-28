package repositories

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
	"time"
)

type KeyRepository struct{}

func NewKeyRepository() *KeyRepository {
	return &KeyRepository{}
}

func (repo *KeyRepository) Count(tx *sql.Tx) (int64, error) {
	var count int64

	sqlStat := "SELECT count(*) FROM redisdata WHERE redisdata.expires_at > now() OR expires_at IS NULL"
	err := tx.QueryRow(sqlStat).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (repo *KeyRepository) Delete(tx *sql.Tx, key []byte) (updated bool, err error) {

	// delete any expired rows in the db with this key
	// we do this first so the count we return at the end doesn't include these rows
	sqlStat := "DELETE FROM redisdata WHERE key=$1 AND expires_at < now()"
	_, err = tx.Exec(sqlStat, key)
	if err != nil {
		return false, err
	}

	sqlStat = "DELETE FROM redisdata WHERE key=$1"
	res, err := tx.Exec(sqlStat, key)
	if err != nil {
		return false, err
	}
	count, err := res.RowsAffected()

	if err != nil {
		return false, err
	}

	return count > 0, nil
}

func (repo *KeyRepository) Exist(tx *sql.Tx, key []byte) (bool, error) {
	var count int

	sqlStat := "SELECT count(*) FROM redisdata WHERE redisdata.key = $1 AND (redisdata.expires_at > now() OR expires_at IS NULL)"
	err := tx.QueryRow(sqlStat, key).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (repo *KeyRepository) SetExpire(tx *sql.Tx, key []byte, expiry_secs int) (updated bool, err error) {
	if expiry_secs <= 0 {
		return false, errors.New("expiry_secs must be 1s or more")
	} else if expiry_secs > 1000000000 {
		return false, errors.New("expiry_secs must be 1,000,000,000 or lower") // that's over 31 years
	}

	sqlStat := "UPDATE redisdata SET expires_at=(now() + cast($2 as interval)) WHERE key=$1 AND (expires_at > now() OR expires_at IS NULL)"
	interval := fmt.Sprintf("%d seconds", expiry_secs)
	res, err := tx.Exec(sqlStat, key, interval)
	if err != nil {
		return false, err
	}
	count, _ := res.RowsAffected()
	updated = count > 0

	if err != nil {
		return false, err
	}
	return updated, nil
}

func (repo *KeyRepository) TTLInMillis(tx *sql.Tx, key []byte) (bool, int64, error) {
	var expiresAt pq.NullTime

	sqlStat := "SELECT expires_at FROM redisdata WHERE key = $1 AND (expires_at > now() OR expires_at IS NULL)"
	row := tx.QueryRow(sqlStat, key)

	switch err := row.Scan(&expiresAt); err {
	case sql.ErrNoRows:
		return false, 0, nil
	case nil:
		// the key is found, but the expiry time is null
		if !expiresAt.Valid {
			return true, 0, nil
		}
		diff := expiresAt.Time.Sub(time.Now()).Milliseconds()
		return true, int64(diff), nil
	default:
		return false, 0, err
	}
}

func (repo *KeyRepository) Type(tx *sql.Tx, key []byte) (string, error) {
	var keyType string

	sqlStat := "SELECT type FROM redisdata WHERE redisdata.key = $1 AND (redisdata.expires_at > now() OR expires_at IS NULL)"
	row := tx.QueryRow(sqlStat, key)

	switch err := row.Scan(&keyType); err {
	case sql.ErrNoRows:
		return "", nil
	case nil:
		return keyType, nil
	default:
		return "", err
	}
}

func (repo *KeyRepository) FlushAll(tx *sql.Tx) error {
	sqlStat := "TRUNCATE redisdata CASCADE"
	_, err := tx.Exec(sqlStat)
	if err != nil {
		return err
	}
	return nil
}
