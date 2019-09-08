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

func (repo *SortedSetRepository) Add(key []byte, values map[string]float64, chArgProvided bool) (updated int64, err error) {
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
		if rowCount == 0 {
			// the set must already have this member, update it with a new score if necessary
			sqlStat = "UPDATE rediszsets SET score = $3 WHERE key = $1 AND value = $2 AND score <> $4"
			res, err := tx.Exec(sqlStat, key, value, score, score)
			if err != nil {
				return 0, err
			}
			if chArgProvided {
				updatedCount, _ := res.RowsAffected()
				count += updatedCount
			}
		} else {
			count += rowCount
		}
	}

	err = tx.Commit()
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (repo *SortedSetRepository) Cardinality(key []byte) (count int64, err error) {
	sqlStat := "SELECT count(*) FROM redisdata INNER JOIN rediszsets ON redisdata.key = rediszsets.key WHERE redisdata.key = $1 AND (redisdata.expires_at > now() OR expires_at IS NULL)"
	err = repo.db.QueryRow(sqlStat, key).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (repo *SortedSetRepository) Range(key []byte, start int, end int, withScores bool) ([]string, error) {
	var setLength int
	result := make([]string, 0)

	tx, err := repo.db.Begin()
	if err != nil {
		return result, err
	}

	sqlStat := "SELECT count(*) FROM redisdata INNER JOIN rediszsets ON redisdata.key = rediszsets.key WHERE redisdata.key = $1 AND (redisdata.expires_at > now() OR expires_at IS NULL)"
	err = tx.QueryRow(sqlStat, key).Scan(&setLength)
	if err != nil {
		return result, err
	}

	// start normalise start/end values
	if start < 0 {
		start = setLength + start
	}

	if end < 0 {
		end = setLength + end
	}

	//end += 1

	if start < 0 {
		start = 0
	}

	if end < start {
		end = start
	}
	// end normalise start/end values

	// The start and end values we have assume a zero-indexed set, but in the database we don't store an index
	// This uses a CTE to select the set values and assign an in-memory zero-index that we can select on
	sqlStat = `
		WITH subset AS (
			SELECT rediszsets.value, score,
			ROW_NUMBER () OVER (ORDER BY score,rediszsets.value)-1 as row
			FROM redisdata INNER JOIN rediszsets ON redisdata.key = rediszsets.key
			WHERE redisdata.key = $1 AND
				(redisdata.expires_at > now() OR expires_at IS NULL)
			)
		SELECT value, score
		FROM subset
		WHERE (row BETWEEN $2 AND $3)
		ORDER BY row
	`
	rows, err := tx.Query(sqlStat, key, start, end)
	if err != nil {
		return result, err
	}
	defer rows.Close()

	for rows.Next() {
		var value string
		var score string
		err = rows.Scan(&value, &score)
		if err != nil {
			return result, err
		}
		result = append(result, value)
		if withScores {
			result = append(result, score)
		}
	}
	err = rows.Err()
	if err != nil {
		return result, err
	}

	// save our work, release all locks
	err = tx.Commit()
	if err != nil {
		return result, err
	}

	return result, nil
}
