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

