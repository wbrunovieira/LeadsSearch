package repository

import (
	"database/sql"
)

func GetStateNameByID(db *sql.DB, stateID string) (string, error) {
    var stateName string
    query := "SELECT name FROM state WHERE id = ?"
    err := db.QueryRow(query, stateID).Scan(&stateName)
    if err != nil {
        return "", err
    }
    return stateName, nil
}
