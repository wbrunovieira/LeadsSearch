package repository

import (
	"database/sql"
	"fmt"
)

// Estrutura para armazenar todos os IDs e nomes que você precisa
type LocationInfo struct {
	ZipcodeID    string
	DistrictID   string
	DistrictName string
	CityID       string
	CityName     string
	StateID      string
	StateName    string
	CountryID    string
	CountryName  string
}

// Função para buscar todos os IDs e nomes com base no zipcodeID
func GetLocationInfoByZipcodeID(db *sql.DB, zipcodeID int) (*LocationInfo, error) {
	query := `
		SELECT 
			z.id AS zipcode_id, d.id AS district_id, d.name AS district_name,
			c.id AS city_id, c.name AS city_name,
			s.id AS state_id, s.name AS state_name,
			co.id AS country_id, co.name AS country_name
		FROM zipcode z
		JOIN district d ON z.district_id = d.id
		JOIN city c ON d.city_id = c.id
		JOIN state s ON c.state_id = s.id
		JOIN country co ON s.country_id = co.id
		WHERE z.id = ?
	`
	var location LocationInfo
	err := db.QueryRow(query, zipcodeID).Scan(
		&location.ZipcodeID, &location.DistrictID, &location.DistrictName,
		&location.CityID, &location.CityName, &location.StateID, &location.StateName,
		&location.CountryID, &location.CountryName,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("No location found for zipcode ID %d", zipcodeID)
		}
		return nil, err
	}
	return &location, nil
}

func GetFirstZipCodeInRange(db *sql.DB, districtID int) (string, error) {
	var startZip string
	err := db.QueryRow("SELECT start_zip FROM zipcode WHERE district_id = ?", districtID).Scan(&startZip)
	if err != nil {
		return "", err
	}
	return startZip, nil
}
