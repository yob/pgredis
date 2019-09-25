package repositories

import (
	"database/sql"
	"errors"
	"fmt"
	"math"
)

type SortedSetRepository struct{}

func NewSortedSetRepository() *SortedSetRepository {
	return &SortedSetRepository{}
}

func (repo *SortedSetRepository) Add(tx *sql.Tx, key []byte, values map[string]float64, chArgProvided bool) (updated int64, err error) {
	count := int64(0)

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

	return count, nil
}

func (repo *SortedSetRepository) Cardinality(tx *sql.Tx, key []byte) (count int64, err error) {
	sqlStat := "SELECT count(*) FROM redisdata INNER JOIN rediszsets ON redisdata.key = rediszsets.key WHERE redisdata.key = $1 AND (redisdata.expires_at > now() OR expires_at IS NULL)"
	err = tx.QueryRow(sqlStat, key).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (repo *SortedSetRepository) Range(tx *sql.Tx, key []byte, start int, end int, direction string, withScores bool) ([]string, error) {
	if direction != "asc" && direction != "desc" {
		return nil, errors.New("direction must be 'asc' or 'desc'")
	}
	var setLength int
	result := make([]string, 0)

	sqlStat := "SELECT count(*) FROM redisdata INNER JOIN rediszsets ON redisdata.key = rediszsets.key WHERE redisdata.key = $1 AND (redisdata.expires_at > now() OR expires_at IS NULL)"
	err := tx.QueryRow(sqlStat, key).Scan(&setLength)
	if err != nil {
		return result, err
	}

	// TODO this start/end logic is *VERY* similair to logic in ListRepository.Lrange, Maybe it could
	// be extracted into a shared internal package?
	// start normalise start/end values
	if start < 0 {
		start = setLength + start
	}

	if end < 0 {
		end = setLength + end
	}
	// end normalise start/end values

	// The start and end values we have assume a zero-indexed set, but in the database we don't store an index
	// This uses a CTE to select the set values and assign an in-memory zero-index that we can select on
	sqlStat = `
		WITH subset AS (
			SELECT rediszsets.value, score,
			ROW_NUMBER () OVER (ORDER BY score %s,rediszsets.value %s)-1 as row
			FROM redisdata INNER JOIN rediszsets ON redisdata.key = rediszsets.key
			WHERE redisdata.key = $1 AND
				(redisdata.expires_at > now() OR expires_at IS NULL)
		)
		SELECT value, score
		FROM subset
		WHERE (row BETWEEN $2 AND $3)
		ORDER BY row
	`
	sqlStat = fmt.Sprintf(sqlStat, direction, direction)
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

	return result, nil
}

func (repo *SortedSetRepository) RangeByScore(tx *sql.Tx, key []byte, min float64, minExclusive bool, max float64, maxExclusive bool, offset int, count int, withScores bool) ([]string, error) {
	var result []string
	var minOperator string
	var maxOperator string
	var rows *sql.Rows
	var err error
	var sqlLimit string

	if minExclusive {
		minOperator = ">"
	} else {
		minOperator = ">="
	}

	if maxExclusive {
		maxOperator = "<"
	} else {
		maxOperator = "<="
	}

	if offset > 0 || count > 0 {
		sqlLimit = fmt.Sprintf(" LIMIT %d OFFSET %d", count, offset)
	} else if count < 0 {
		sqlLimit = fmt.Sprintf(" OFFSET %d", offset)
	} else {
		sqlLimit = ""
	}

	// The start and end values we have assume a zero-indexed set, but in the database we don't store an index
	// This uses a CTE to select the set values and assign an in-memory zero-index that we can select on
	if math.IsInf(min, 0) && math.IsInf(max, 0) {
		sqlStat := "SELECT rediszsets.value, score FROM redisdata INNER JOIN rediszsets ON redisdata.key = rediszsets.key WHERE redisdata.key = $1 AND (redisdata.expires_at > now() OR expires_at IS NULL) ORDER BY rediszsets.score, rediszsets.value" + sqlLimit
		rows, err = tx.Query(sqlStat, key)
	} else if !math.IsInf(min, 0) && !math.IsInf(max, 0) {
		sqlStat := fmt.Sprintf("SELECT rediszsets.value, score FROM redisdata INNER JOIN rediszsets ON redisdata.key = rediszsets.key WHERE redisdata.key = $1 AND score %s $2 AND score %s $3 AND (redisdata.expires_at > now() OR expires_at IS NULL) ORDER BY rediszsets.score, rediszsets.value", minOperator, maxOperator) + sqlLimit
		rows, err = tx.Query(sqlStat, key, min, max)
	} else if math.IsInf(min, 0) {
		sqlStat := fmt.Sprintf("SELECT rediszsets.value, score FROM redisdata INNER JOIN rediszsets ON redisdata.key = rediszsets.key WHERE redisdata.key = $1 AND score %s $2 AND (redisdata.expires_at > now() OR expires_at IS NULL) ORDER BY rediszsets.score, rediszsets.value", maxOperator) + sqlLimit
		rows, err = tx.Query(sqlStat, key, max)
	} else if math.IsInf(max, 0) {
		sqlStat := fmt.Sprintf("SELECT rediszsets.value,score FROM redisdata INNER JOIN rediszsets ON redisdata.key = rediszsets.key WHERE redisdata.key = $1 AND score %s $2 AND (redisdata.expires_at > now() OR expires_at IS NULL) ORDER BY rediszsets.score, rediszsets.value", minOperator) + sqlLimit
		rows, err = tx.Query(sqlStat, key, min)
	}

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

	return result, nil
}

func (repo *SortedSetRepository) Remove(tx *sql.Tx, key []byte, values [][]byte) (count int64, err error) {
	var lockedKey string

	// delete any expired rows in the db with this key
	// we do this first so the count we return at the end doesn't include these rows
	sqlStat := "DELETE FROM redisdata WHERE key=$1 AND expires_at < now()"
	_, err = tx.Exec(sqlStat, key)
	if err != nil {
		return 0, err
	}

	// now lock that key so no one else can change it
	sqlStat = "SELECT key FROM redisdata WHERE redisdata.key = $1 AND (redisdata.expires_at > now() OR expires_at IS NULL) FOR UPDATE"
	err = tx.QueryRow(sqlStat, key).Scan(&lockedKey)

	if err == sql.ErrNoRows {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	// Delete each member
	// TODO presumably there's a way to do this in a single SQL statement?
	for _, value := range values {
		sqlStat = "DELETE FROM rediszsets WHERE key=$1 AND value = $2"
		res, err := tx.Exec(sqlStat, key, value)
		if err != nil {
			return 0, err
		}
		rowCount, _ := res.RowsAffected()
		count += rowCount
	}

	// if the set is now empty, delete it
	var remainingMembers int64
	sqlStat = "SELECT count(*) FROM redisdata INNER JOIN rediszsets ON redisdata.key = rediszsets.key WHERE redisdata.key = $1"
	err = tx.QueryRow(sqlStat, key).Scan(&remainingMembers)

	if err != nil {
		return 0, err
	}

	if remainingMembers == 0 {
		sqlStat = "DELETE FROM redisdata WHERE key=$1"
		_, err := tx.Exec(sqlStat, key)
		if err != nil {
			return 0, err
		}
	}

	return count, nil
}

func (repo *SortedSetRepository) RemoveRangeByRank(tx *sql.Tx, key []byte, start int, end int) (count int64, err error) {
	var lockedKey string
	var setLength int

	// delete any expired rows in the db with this key
	// we do this first so the count we return at the end doesn't include these rows
	sqlStat := "DELETE FROM redisdata WHERE key=$1 AND expires_at < now()"
	_, err = tx.Exec(sqlStat, key)
	if err != nil {
		return 0, err
	}

	// now lock that key so no one else can change it
	sqlStat = "SELECT key FROM redisdata WHERE redisdata.key = $1 AND (redisdata.expires_at > now() OR expires_at IS NULL) FOR UPDATE"
	err = tx.QueryRow(sqlStat, key).Scan(&lockedKey)

	if err == sql.ErrNoRows {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	sqlStat = "SELECT count(*) FROM redisdata INNER JOIN rediszsets ON redisdata.key = rediszsets.key WHERE redisdata.key = $1 AND (redisdata.expires_at > now() OR expires_at IS NULL)"
	err = tx.QueryRow(sqlStat, key).Scan(&setLength)
	if err != nil {
		return 0, err
	}

	// TODO this start/end logic is *VERY* similar to logic in ListRepository.Lrange, Maybe it could
	// be extracted into a shared internal package?
	// start normalise start/end values
	if start < 0 {
		start = setLength + start
	}

	if end < 0 {
		end = setLength + end
	}
	// end normalise start/end values

	// The start and end values we have assume a zero-indexed set, but in the database we don't store an index
	// This uses a CTE to select the set values and assign an in-memory zero-index that we can select on
	sqlStat = `
		WITH subset AS (
			SELECT rediszsets.ctid, rediszsets.value, score,
			ROW_NUMBER () OVER (ORDER BY score,rediszsets.value)-1 as row
			FROM redisdata INNER JOIN rediszsets ON redisdata.key = rediszsets.key
			WHERE redisdata.key = $1 AND
				(redisdata.expires_at > now() OR expires_at IS NULL)
		)
		DELETE FROM rediszsets
		WHERE key = $1 AND ctid IN (
			SELECT ctid from subset WHERE row >= $2 AND row <= $3
		)
	`
	res, err := tx.Exec(sqlStat, key, start, end)
	if err != nil {
		return 0, err
	}
	rowCount, _ := res.RowsAffected()
	count += rowCount

	// if the set is now empty, delete it
	var remainingMembers int64
	sqlStat = "SELECT count(*) FROM redisdata INNER JOIN rediszsets ON redisdata.key = rediszsets.key WHERE redisdata.key = $1"
	err = tx.QueryRow(sqlStat, key).Scan(&remainingMembers)

	if err != nil {
		return 0, err
	}

	if remainingMembers == 0 {
		sqlStat = "DELETE FROM redisdata WHERE key=$1"
		_, err := tx.Exec(sqlStat, key)
		if err != nil {
			return 0, err
		}
	}

	return count, nil
}

func (repo *SortedSetRepository) RemoveRangeByScore(tx *sql.Tx, key []byte, min float64, minExclusive bool, max float64, maxExclusive bool) (count int64, err error) {
	var lockedKey string
	var res sql.Result
	var minOperator string
	var maxOperator string

	if minExclusive {
		minOperator = ">"
	} else {
		minOperator = ">="
	}

	if maxExclusive {
		maxOperator = "<"
	} else {
		maxOperator = "<="
	}

	// delete any expired rows in the db with this key
	// we do this first so the count we return at the end doesn't include these rows
	sqlStat := "DELETE FROM redisdata WHERE key=$1 AND expires_at < now()"
	_, err = tx.Exec(sqlStat, key)
	if err != nil {
		return 0, err
	}

	// now lock that key so no one else can change it
	sqlStat = "SELECT key FROM redisdata WHERE redisdata.key = $1 AND (redisdata.expires_at > now() OR expires_at IS NULL) FOR UPDATE"
	err = tx.QueryRow(sqlStat, key).Scan(&lockedKey)

	if err == sql.ErrNoRows {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	// The start and end values we have assume a zero-indexed set, but in the database we don't store an index
	// This uses a CTE to select the set values and assign an in-memory zero-index that we can select on
	if math.IsInf(min, 0) && math.IsInf(max, 0) {
		sqlStat = "DELETE FROM rediszsets WHERE key = $1"
		res, err = tx.Exec(sqlStat, key)
	} else if !math.IsInf(min, 0) && !math.IsInf(max, 0) {
		sqlStat = fmt.Sprintf("DELETE FROM rediszsets WHERE key = $1 AND score %s $2 AND score %s $3", minOperator, maxOperator)
		res, err = tx.Exec(sqlStat, key, min, max)
	} else if math.IsInf(min, 0) {
		sqlStat = fmt.Sprintf("DELETE FROM rediszsets WHERE key = $1 AND score %s $2", maxOperator)
		res, err = tx.Exec(sqlStat, key, max)
	} else if math.IsInf(max, 0) {
		sqlStat = fmt.Sprintf("DELETE FROM rediszsets WHERE key = $1 AND score %s $2", minOperator)
		res, err = tx.Exec(sqlStat, key, min)
	}
	if err != nil {
		return 0, err
	}
	rowCount, _ := res.RowsAffected()
	count += rowCount

	// if the set is now empty, delete it
	var remainingMembers int64
	sqlStat = "SELECT count(*) FROM redisdata INNER JOIN rediszsets ON redisdata.key = rediszsets.key WHERE redisdata.key = $1"
	err = tx.QueryRow(sqlStat, key).Scan(&remainingMembers)

	if err != nil {
		return 0, err
	}

	if remainingMembers == 0 {
		sqlStat = "DELETE FROM redisdata WHERE key=$1"
		_, err := tx.Exec(sqlStat, key)
		if err != nil {
			return 0, err
		}
	}

	return count, nil
}
