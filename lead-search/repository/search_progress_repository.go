package repository

import (
	"database/sql"
	"fmt"
)

type SearchProgress struct {
	CategoriaID string
	CountryID   string
	StateID     string
	CityID      string
	DistrictID  string
	ZipcodeID   string
	Radius      int
	SearchDone  int // 0 = Não concluído, 1 = Concluído
}


func InsertSearchProgress(db *sql.DB, progress SearchProgress) (int64, error) {
	query := `
		INSERT INTO search_progress (categoria_id, country_id, state_id, city_id, district_id, zipcode_id, search_done)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`
	result, err := db.Exec(query, progress.CategoriaID, progress.CountryID, progress.StateID, progress.CityID, progress.DistrictID, progress.ZipcodeID, progress.SearchDone)
	if err != nil {
		return 0, fmt.Errorf("failed to insert search progress: %v", err)
	}

	
	return result.LastInsertId()
}



func UpdateSearchProgressPage(db *sql.DB, progressID int64, currentPage int) error {
	query := `
		UPDATE search_progress
		SET search_done = ?
		WHERE id = ?
	`
	_, err := db.Exec(query, currentPage, progressID)
	if err != nil {
		return fmt.Errorf("failed to update search progress: %v", err)
	}
	return nil
}

