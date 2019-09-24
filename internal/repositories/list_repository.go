package repositories

import (
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/lib/pq"
)

type ListRepository struct{}

func NewListRepository() *ListRepository {
	return &ListRepository{}
}

func (repo *ListRepository) Length(tx *sql.Tx, key []byte) (int, error) {
	var count int

	sqlStat := "SELECT count(*) FROM redisdata INNER JOIN redislists ON redisdata.key = redislists.key WHERE redisdata.key = $1 AND (redisdata.expires_at > now() OR expires_at IS NULL)"
	err := tx.QueryRow(sqlStat, key).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (repo *ListRepository) LeftPop(tx *sql.Tx, key []byte) (bool, []byte, error) {
	return repo.pop(tx, key, "left")
}

func (repo *ListRepository) LeftPush(tx *sql.Tx, key []byte, values [][]byte) (int, error) {
	return repo.push(tx, key, "left", values)
}

func (repo *ListRepository) Lrange(tx *sql.Tx, key []byte, start int, end int) ([]string, error) {
	var listLength int
	result := make([]string, 0)

	sqlStat := "SELECT count(*) FROM redisdata INNER JOIN redislists ON redisdata.key = redislists.key WHERE redisdata.key = $1 AND (redisdata.expires_at > now() OR expires_at IS NULL)"
	err := tx.QueryRow(sqlStat, key).Scan(&listLength)
	if err != nil {
		return result, err
	}

	// TODO this start/end logic is *VERY* similair to logic in SortedSetRepository.Range, Maybe it could
	// be extracted into a shared internal package?
	// start normalise start/end values
	if start < 0 {
		start = listLength + start
	}

	if end < 0 {
		end = listLength + end
	}
	// end normalise start/end values

	// The start and end values we have assume a zero-indexed list, but in the database our index values aren't zero indexed.
	// This uses a CTE to select the list values and assign an in-memory zero-index that we can select on
	sqlStat = `
		WITH sublist AS (
			SELECT redislists.value,
			ROW_NUMBER () OVER (ORDER BY idx)-1 as row
			FROM redisdata INNER JOIN redislists ON redisdata.key = redislists.key
			WHERE redisdata.key = $1 AND
				(redisdata.expires_at > now() OR expires_at IS NULL)
			)
		SELECT value
		FROM sublist
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

func (repo *ListRepository) LeftRemove(tx *sql.Tx, key []byte, count int, value []byte) (int64, error) {
	var removedCount int64
	var maxIdx sql.NullInt64

	// delete any expired rows in the db with this key
	sqlStat := "DELETE FROM redisdata WHERE key=$1 AND expires_at < now()"
	_, err := tx.Exec(sqlStat, key)
	if err != nil {
		return 0, err
	}

	// now lock that key so no one else can change it
	sqlStat = "SELECT key FROM redisdata WHERE redisdata.key = $1 AND (redisdata.expires_at > now() OR expires_at IS NULL) FOR UPDATE"
	_, err = tx.Exec(sqlStat, key)

	if err != nil {
		return 0, err
	}

	// determine the maximum index that has a matching value
	sqlStat = "SELECT max(idx) FROM (SELECT idx FROM redisdata INNER JOIN redislists ON redisdata.key = redislists.key WHERE redisdata.key = $1 AND (redisdata.expires_at > now() OR expires_at IS NULL) AND redislists.value = $2 ORDER BY idx limit $3) t"
	err = tx.QueryRow(sqlStat, key, value, count).Scan(&maxIdx)

	if err != nil {
		return 0, err
	}
	if !maxIdx.Valid {
		return 0, nil
	}

	// delete the list items
	sqlStat = "DELETE FROM redislists WHERE key=$1 AND idx <= $2 AND value = $3"
	res, err := tx.Exec(sqlStat, key, maxIdx, value)
	if err != nil {
		return 0, err
	}
	removedCount, _ = res.RowsAffected()

	return removedCount, nil
}

func (repo *ListRepository) RightPop(tx *sql.Tx, key []byte) (bool, []byte, error) {
	return repo.pop(tx, key, "right")
}

func (repo *ListRepository) RightPush(tx *sql.Tx, key []byte, values [][]byte) (int, error) {
	return repo.push(tx, key, "right", values)
}

func (repo *ListRepository) pop(tx *sql.Tx, key []byte, direction string) (bool, []byte, error) {
	var value []byte

	if direction != "left" && direction != "right" {
		return false, value, errors.New("direction must be left or right")
	}

	// delete any expired rows in the db with this key
	sqlStat := "DELETE FROM redisdata WHERE key=$1 AND expires_at < now()"
	_, err := tx.Exec(sqlStat, key)
	if err != nil {
		return false, value, err
	}

	// now lock that key so no one else can change it
	sqlStat = "SELECT key FROM redisdata WHERE redisdata.key = $1 AND (redisdata.expires_at > now() OR expires_at IS NULL) FOR UPDATE"
	_, err = tx.Exec(sqlStat, key)

	if err != nil {
		return false, value, err
	}

	// delete the list items
	sqlStat = `
		WITH sublist AS (
			SELECT redislists.key, redislists.idx, redislists.value
			FROM redisdata INNER JOIN redislists ON redisdata.key = redislists.key
			WHERE redisdata.key = $1 AND
				(redisdata.expires_at > now() OR expires_at IS NULL)
			ORDER BY redislists.idx %s
			LIMIT 1
		)
		DELETE
		FROM redislists
		WHERE key IN (SELECT key FROM sublist) AND idx IN (SELECT idx FROM sublist) AND value IN (SELECT value FROM sublist)
		RETURNING value
	`
	if direction == "left" {
		sqlStat = fmt.Sprintf(sqlStat, "asc")
	} else {
		sqlStat = fmt.Sprintf(sqlStat, "desc")
	}
	err = tx.QueryRow(sqlStat, key).Scan(&value)
	if err == sql.ErrNoRows {
		return false, value, nil
	} else if err != nil {
		return false, value, err
	}

	// if the list is now empty, delete it
	var remainingItems int64
	sqlStat = "SELECT count(*) FROM redisdata INNER JOIN redislists ON redisdata.key = redislists.key WHERE redisdata.key = $1"
	err = tx.QueryRow(sqlStat, key).Scan(&remainingItems)

	if err != nil {
		return false, value, err
	}

	if remainingItems == 0 {
		sqlStat = "DELETE FROM redisdata WHERE key=$1"
		_, err := tx.Exec(sqlStat, key)
		if err != nil {
			return false, value, err
		}
	}

	return true, value, nil
}

func (repo *ListRepository) push(tx *sql.Tx, key []byte, direction string, values [][]byte) (int, error) {
	if direction != "left" && direction != "right" {
		return 0, errors.New("direction must be left or right")
	}
	var newLength int

	// delete any expired rows in the db with this key
	sqlStat := "DELETE FROM redisdata WHERE key=$1 AND expires_at < now()"
	_, err := tx.Exec(sqlStat, key)
	if err != nil {
		return 0, err
	}

	// ensure the db has a current key
	sqlStat = "INSERT INTO redisdata(key, type, value, expires_at) VALUES ($1, 'list', '', NULL) ON CONFLICT (key) DO NOTHING"
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

	if direction == "left" {
		sqlStat = "INSERT INTO redislists(key, idx, value) VALUES ($1, (select coalesce(min(idx),0) from redislists where key = $2)-1, $3)"
	} else {
		sqlStat = "INSERT INTO redislists(key, idx, value) VALUES ($1, (select coalesce(max(idx),0) from redislists where key = $2)+1, $3)"
	}
	// append our new values
	for _, value := range values {
		_, err = tx.Exec(sqlStat, key, key, value)

		if err != nil {
			return 0, err
		}
	}

	sqlStat = "SELECT count(*) FROM redisdata INNER JOIN redislists ON redisdata.key = redislists.key WHERE redisdata.key = $1"
	err = tx.QueryRow(sqlStat, key).Scan(&newLength)
	if err != nil {
		return 0, err
	}

	return newLength, nil
}
