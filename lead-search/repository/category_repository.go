package repository

import (
    "database/sql"
    "fmt"
)

// Função para buscar o nome da categoria com base no ID
func GetCategoryNameByID(db *sql.DB, categoryID string) (string, error) {
    var categoryName string
    query := "SELECT nome FROM categoria WHERE id = ?"
    err := db.QueryRow(query, categoryID).Scan(&categoryName)
    if err != nil {
        if err == sql.ErrNoRows {
            return "", fmt.Errorf("Category with ID %s not found", categoryID)
        }
        return "", err
    }
    return categoryName, nil
}
