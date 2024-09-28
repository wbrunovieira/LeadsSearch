package repository

import (
	"database/sql"
)

func GetCityNameByID(db *sql.DB, cityID string) (string, error) {
    var cityName string
    query := "SELECT name FROM city WHERE id = ?"
    err := db.QueryRow(query, cityID).Scan(&cityName)
    if err != nil {
        return "", err
    }
    return cityName, nil
}
