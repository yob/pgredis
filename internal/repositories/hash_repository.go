package repositories

import (
	"database/sql"
)

type HashRepository struct{}

func NewHashRepository() *HashRepository {
	return &HashRepository{}
}

func (repo *HashRepository) Get(tx *sql.Tx, key []byte, field []byte) (success bool, value []byte, err error) {

	sqlStat := `
			SELECT redishashes.value
			FROM redisdata INNER JOIN redishashes ON redisdata.key = redishashes.key
			WHERE redisdata.key = $1 AND
				redishashes.field = $2 AND
				(redisdata.expires_at > now() OR expires_at IS NULL)
	`

	row := tx.QueryRow(sqlStat, key, field)

	switch err := row.Scan(&value); err {
	case sql.ErrNoRows:
		return false, value, nil
	case nil:
		return true, value, nil
	default:
		return false, value, err
	}
}

func (repo *HashRepository) GetAll(tx *sql.Tx, key []byte) (fields_and_values []string, err error) {
	fields_and_values = []string{}
	sqlStat := `
			SELECT redishashes.field, redishashes.value
			FROM redisdata INNER JOIN redishashes ON redisdata.key = redishashes.key
			WHERE redisdata.key = $1 AND
				(redisdata.expires_at > now() OR expires_at IS NULL)
	`

	rows, err := tx.Query(sqlStat, key)
	if err != nil {
		return fields_and_values, err
	}
	defer rows.Close()

	for rows.Next() {
		var field string
		var value string
		err = rows.Scan(&field, &value)
		if err != nil {
			return fields_and_values, err
		}
		fields_and_values = append(fields_and_values, field, value)
	}
	err = rows.Err()
	if err != nil {
		return fields_and_values, err
	}
	return fields_and_values, nil
}

func (repo *HashRepository) Set(tx *sql.Tx, key []byte, field []byte, value []byte) (inserted int64, err error) {

	// take an exclusive lock for this key
	sqlStat := "SELECT pg_advisory_xact_lock(hashtext($1))"
	_, err = tx.Exec(sqlStat, key)
	if err != nil {
		return 0, err
	}

	// delete any expired rows in the db with this key
	sqlStat = "DELETE FROM redisdata WHERE key=$1 AND expires_at < now()"
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

	return inserted, nil
}

func (repo *HashRepository) SetMultiple(tx *sql.Tx, key string, fields_and_values map[string]string) (err error) {

	// take an exclusive lock for this key
	sqlStat := "SELECT pg_advisory_xact_lock(hashtext($1))"
	_, err = tx.Exec(sqlStat, key)
	if err != nil {
		return err
	}

	// delete any expired rows in the db with this key
	sqlStat = "DELETE FROM redisdata WHERE key=$1 AND expires_at < now()"
	_, err = tx.Exec(sqlStat, key)
	if err != nil {
		return err
	}

	// ensure the db has a current key
	sqlStat = "INSERT INTO redisdata(key, type, value, expires_at) VALUES ($1, 'hash', '', NULL) ON CONFLICT (key) DO NOTHING"
	_, err = tx.Exec(sqlStat, key)

	if err != nil {
		return err
	}

	// now lock that key so no one else can change it
	sqlStat = "SELECT key FROM redisdata WHERE redisdata.key = $1 AND (redisdata.expires_at > now() OR expires_at IS NULL) FOR UPDATE"
	_, err = tx.Exec(sqlStat, key)

	if err != nil {
		return err
	}

	for field, value := range fields_and_values {
		// TODO could we do this in a single SQL statement?
		sqlStat = "INSERT INTO redishashes (key, field, value) values ($1, $2, $3) ON CONFLICT (key, field) DO UPDATE SET value=$3"
		_, err := tx.Exec(sqlStat, key, field, value)
		if err != nil {
			return err
		}
	}

	return nil
}
