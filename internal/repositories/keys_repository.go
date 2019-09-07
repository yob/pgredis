package repositories

import (
	"database/sql"
	"errors"
	"fmt"
)

type KeyRepository struct {
	db       *sql.DB
}

func NewKeyRepository(db *sql.DB) *KeyRepository {
	return &KeyRepository{
		db: db,
	}
}

func (repo *KeyRepository) Delete(key []byte) (updated bool, err error) {
	tx, err := repo.db.Begin()
	if err != nil {
		return false, err
	}

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

	err = tx.Commit()
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

func (repo *KeyRepository) Exist(key []byte) (bool, error) {
	var count int

	sqlStat := "SELECT count(*) FROM redisdata WHERE redisdata.key = $1 AND (redisdata.expires_at > now() OR expires_at IS NULL)"
	err := repo.db.QueryRow(sqlStat, key).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (repo *KeyRepository) SetExpire(key []byte, expiry_millis int) (updated bool, err error) {
	if expiry_millis <= 0 {
		return false, errors.New("expiry must be 1ms or more")
	}

	sqlStat := "UPDATE redisdata SET expires_at=(now() + cast($2 as interval)) WHERE key=$1 AND (expires_at > now() OR expires_at IS NULL)"
	interval := fmt.Sprintf("%d milliseconds", expiry_millis)
	res, err := repo.db.Exec(sqlStat, key, interval)
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

