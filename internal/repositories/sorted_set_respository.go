package repositories

import (
	"database/sql"
)

type SortedSetRepository struct {
	db *sql.DB
}

func NewSortedSetRepository(db *sql.DB) *SortedSetRepository {
	return &SortedSetRepository{
		db: db,
	}
}

func (repo *SortedSetRepository) Add(key []byte, values map[string]float64) (updated int64, err error) {
	count := int64(0)

	tx, err := repo.db.Begin()
	if err != nil {
		return 0, err
	}

	// delete any expired rows in the db with this key
	// we do this first so the count we return at the end doesn't include these rows
	sqlStat := "DELETE FROM redisdata WHERE key=$1 AND expires_at < now()"
	_, err = tx.Exec(sqlStat, key)
	if err != nil {
		return 0, err
	}

	// ensure the db has a current key
	sqlStat = "INSERT INTO redisdata(key, type, value, expires_at) VALUES ($1, 'zset', '', NULL) ON CONFLICT (key) DO NOTHING"
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

	for value, score := range values {
		sqlStat = "INSERT INTO rediszsets(key, value, score) VALUES ($1, $2, $3) ON CONFLICT (key, value) DO NOTHING"
		res, err := tx.Exec(sqlStat, key, value, score)
		if err != nil {
			return 0, err
		}
		rowCount, _ := res.RowsAffected()
		count += rowCount
	}

	err = tx.Commit()
	if err != nil {
		return 0, err
	}

	return count, nil
}
