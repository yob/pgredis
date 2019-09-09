package repositories

import (
	"database/sql"
)

type SetRepository struct {
	db *sql.DB
}

func NewSetRepository(db *sql.DB) *SetRepository {
	return &SetRepository{
		db: db,
	}
}

func (repo *SetRepository) Add(key []byte, values [][]byte) (updated int64, err error) {
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
	sqlStat = "INSERT INTO redisdata(key, type, value, expires_at) VALUES ($1, 'set', '', NULL) ON CONFLICT (key) DO NOTHING"
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

	for _, value := range values {
		sqlStat = "INSERT INTO redissets(key, value) VALUES ($1, $2) ON CONFLICT (key, value) DO NOTHING"
		res, err := tx.Exec(sqlStat, key, value)
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

func (repo *SetRepository) Cardinality(key []byte) (count int64, err error) {
	sqlStat := "SELECT count(*) FROM redisdata INNER JOIN redissets ON redisdata.key = redissets.key WHERE redisdata.key = $1 AND (redisdata.expires_at > now() OR expires_at IS NULL)"
	err = repo.db.QueryRow(sqlStat, key).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (repo *SetRepository) Members(key []byte) (values []string, err error) {
	result := make([]string, 0)

	sqlStat := `
			SELECT redissets.value
			FROM redisdata INNER JOIN redissets ON redisdata.key = redissets.key
			WHERE redisdata.key = $1 AND
				(redisdata.expires_at > now() OR expires_at IS NULL)
	`
	rows, err := repo.db.Query(sqlStat, key)
	if err != nil {
		return result, err
	}
	defer rows.Close()

	for rows.Next() {
		var value string
		err = rows.Scan(&value)
		if err != nil {
			return result, err
		}
		result = append(result, value)
	}
	err = rows.Err()
	if err != nil {
		return result, err
	}

	return result, nil

}
