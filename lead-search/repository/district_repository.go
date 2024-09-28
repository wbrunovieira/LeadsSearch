package repository

import (
    "database/sql"
    "fmt"
)

func GetDistrictNameByID(db *sql.DB, districtID string) (string, error) {
    var districtName string
    query := "SELECT name FROM district WHERE id = ?"
    err := db.QueryRow(query, districtID).Scan(&districtName)
    if err != nil {
        if err == sql.ErrNoRows {
            return "", fmt.Errorf("District with ID %s not found", districtID)
        }
        return "", err
    }
    return districtName, nil
}
