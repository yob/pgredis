package repositories

import (
	"database/sql"

	_ "github.com/lib/pq"
)

type ListRepository struct {
	db       *sql.DB
}

func NewListRepository(db *sql.DB) *ListRepository {
	return &ListRepository{
		db: db,
	}
}

func (repo *ListRepository) Length(key []byte) (int, error) {
	var count int

	sqlStat := "SELECT count(*) FROM redisdata INNER JOIN redislists ON redisdata.key = redislists.key WHERE redisdata.key = $1 AND (redisdata.expires_at > now() OR expires_at IS NULL)"
	row := repo.db.QueryRow(sqlStat, key)

	switch err := row.Scan(&count); err {
	case sql.ErrNoRows:
		return 0, nil
	case nil:
		return 0, nil
	default:
		return count, err
	}
}

func (repo *ListRepository) RightPush(key []byte, values [][]byte) (int, error) {
	var newLength int

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
	sqlStat = "INSERT INTO redisdata(key, value, expires_at) VALUES ($1, '', NULL) ON CONFLICT (key) DO NOTHING"
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

	// append our new values
	for _, value := range values {
		sqlStat = "INSERT INTO redislists(key, idx, value) VALUES ($1, (select count(*) from redislists where key = $2)+1, $3)"
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

	// save our work, release all locks
	err = tx.Commit()
	if err != nil {
		return 0, err
	}

	return newLength, nil
}
