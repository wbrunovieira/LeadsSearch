package repository

import (
	"database/sql"
)

func GetCountryNameByID(db *sql.DB, countryID string) (string, error) {
    var countryName string
    query := "SELECT name FROM country WHERE id = ?"
    err := db.QueryRow(query, countryID).Scan(&countryName)
    if err != nil {
        return "", err
    }
    return countryName, nil
}
