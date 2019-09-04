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

