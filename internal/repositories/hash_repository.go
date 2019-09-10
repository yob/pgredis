package repositories

import (
	"database/sql"
)

type HashRepository struct {
	db *sql.DB
}

func NewHashRepository(db *sql.DB) *HashRepository {
	return &HashRepository{
		db: db,
	}
}

func (repo *HashRepository) Set(key []byte, field []byte, value []byte) (inserted int64, err error) {
	tx, err := repo.db.Begin()
	if err != nil {
		return 0, err
	}

	// delete any expired rows in the db with this key
	sqlStat := "DELETE FROM redisdata WHERE key=$1 AND expires_at < now()"
	_, err = tx.Exec(sqlStat, key)
	if err != nil {
		return 0, err
	}

	// ensure the db has a current key
	sqlStat = "INSERT INTO redisdata(key, type, value, expires_at) VALUES ($1, 'hash', '', NULL) ON CONFLICT (key) DO NOTHING"
	_, err = tx.Exec(sqlStat, key)

	if err != nil {
		return 0, err
	}

	// now lock that key so no one else can change it
	sqlStat = "SELECT key FROM redisdata WHERE redisdata.key = $1 AND (redisdata.expires_at > now() OR expires_at IS NULL) FOR UPDATE"
	_, err = tx.Exec(sqlStat, key)

	if err != nil {
		return 0, err
	}

	// first, try and update an existing field
	sqlStat = "UPDATE redishashes SET value = $3 WHERE key = $1 and field = $2"
	res, err := tx.Exec(sqlStat, key, field, value)
	if err != nil {
		return 0, err
	}
	rowCount, _ := res.RowsAffected()

	// no updates, so should be safe to insert
	if rowCount == 0 {
		sqlStat = "INSERT INTO redishashes (key, field, value) values ($1, $2, $3)"
		res, err := tx.Exec(sqlStat, key, field, value)
		if err != nil {
			return 0, err
		}
		inserted, _ = res.RowsAffected()
	}

	// save our work, release all locks
	err = tx.Commit()
	if err != nil {
		return 0, err
	}

	return inserted, nil
}
