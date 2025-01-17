package googleplaces

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
)

type Service struct {
	APIKey string
}

type TokenStore struct {
	QueryTokens map[string]map[string]interface{} `json:"query_tokens"`
}

type PlaceResult struct {
	Name string `json:"name"`

	FormattedAddress  string   `json:"formatted_address"`
	PlaceID           string   `json:"place_id"`
	Rating            float64  `json:"rating"`
	UserRatingsTotal  int      `json:"user_ratings_total"`
	PriceLevel        int      `json:"price_level"`
	BusinessStatus    string   `json:"business_status"`
	Vicinity          string   `json:"vicinity"`
	PermanentlyClosed bool     `json:"permanently_closed"`
	Types             []string `json:"types"`
}

func NewService(apiKey string) *Service {
	return &Service{APIKey: apiKey}
}

func (s *Service) GeocodeZip(zipCode string) (string, error) {
	log.Printf("Buscando coordenadas para o zipCode: %s", zipCode)

	client := resty.New()

	geocodeURL := "https://maps.googleapis.com/maps/api/geocode/json"
	resp, err := client.R().
		SetQueryParams(map[string]string{
			"address": zipCode,
			"key":     s.APIKey,
		}).
		Get(geocodeURL)

	if err != nil {
		return "", fmt.Errorf("error connecting to Geocoding API: %v", err)
	}

	var result struct {
		Results []struct {
			Geometry struct {
				Location struct {
					Lat float64 `json:"lat"`
					Lng float64 `json:"lng"`
				} `json:"location"`
			} `json:"geometry"`
		} `json:"results"`
		Status       string `json:"status"`
		ErrorMessage string `json:"error_message"`
	}

	err = json.Unmarshal(resp.Body(), &result)
	if err != nil {
		return "", fmt.Errorf("error parsing geocode response: %v", err)
	}

	if result.Status != "OK" {
		return "", fmt.Errorf("geocoding API error: %s, message: %s", result.Status, result.ErrorMessage)
	}

	if len(result.Results) > 0 {
		lat := result.Results[0].Geometry.Location.Lat
		lng := result.Results[0].Geometry.Location.Lng
		return fmt.Sprintf("%f,%f", lat, lng), nil
	}

	return "", fmt.Errorf("no results found for zipCode: %s", zipCode)
}

func generateQueryKey(query string, location string, radius int) string {
	return fmt.Sprintf("%s|%s|%d", query, location, radius)
}

func loadToken(queryKey string) (string, int, int, error) {

	const directory = "/app/lead-search"
	const filePath = directory + "/next_page_tokens.json"

	var tokenStore TokenStore

	file, err := os.ReadFile(filePath)
	if err != nil {

		if os.IsNotExist(err) {
			log.Printf("Arquivo %s não encontrado. Criando um novo...", filePath)

			tokenStore = TokenStore{QueryTokens: make(map[string]map[string]interface{})}

			if _, dirErr := os.Stat(directory); os.IsNotExist(dirErr) {
				log.Printf("Diretório %s não encontrado. Criando...", directory)
				dirErr = os.MkdirAll(directory, os.ModePerm)
				if dirErr != nil {
					return "", 0, 0, fmt.Errorf("erro ao criar o diretório %s: %v", directory, dirErr)
				}
			}

			tokenStoreBytes, jsonErr := json.MarshalIndent(tokenStore, "", "  ")
			if jsonErr != nil {
				return "", 0, 0, fmt.Errorf("erro ao serializar tokens: %v", jsonErr)
			}
			err = os.WriteFile(filePath, tokenStoreBytes, 0644)
			if err != nil {
				return "", 0, 0, fmt.Errorf("erro ao criar o arquivo JSON vazio: %v", err)
			}

			return "", 0, 0, nil
		}
		return "", 0, 0, fmt.Errorf("erro ao ler o arquivo JSON: %v", err)
	}

	if len(strings.TrimSpace(string(file))) == 0 {
		tokenStore = TokenStore{QueryTokens: make(map[string]map[string]interface{})}
	} else {

		err = json.Unmarshal(file, &tokenStore)
		if err != nil {
			return "", 0, 0, fmt.Errorf("erro ao fazer parse do arquivo JSON: %v", err)
		}
	}

	if queryData, exists := tokenStore.QueryTokens[queryKey]; exists {

		token, tokenOk := queryData["next_page_token"].(string)
		pagesFetched, pagesOk := queryData["pages_fetched"].(float64)
		leadsExtracted, leadsOk := queryData["leads_extracted"].(float64)

		if tokenOk && pagesOk && leadsOk {
			return token, int(pagesFetched), int(leadsExtracted), nil
		}
		return "", 0, 0, fmt.Errorf("erro ao converter valores: %v", queryData)
	}

	// Retorna valores padrão se a chave não existir
	return "", 0, 0, nil
}

func saveToken(queryKey string, token string, pagesFetched int, leadsExtracted int) error {
	var tokenStore TokenStore

	file, err := os.ReadFile("next_page_tokens.json")
	if err == nil {
		err = json.Unmarshal(file, &tokenStore)
		if err != nil {
			return fmt.Errorf("erro ao fazer parse do arquivo JSON: %v", err)
		}
	} else {
		tokenStore = TokenStore{QueryTokens: make(map[string]map[string]interface{})}
	}

	if tokenStore.QueryTokens == nil {
		log.Println("Mapa QueryTokens não está inicializado. Inicializando agora.")
		tokenStore.QueryTokens = make(map[string]map[string]interface{})
	}

	tokenStore.QueryTokens[queryKey] = map[string]interface{}{
		"next_page_token": token,
		"pages_fetched":   pagesFetched,
		"leads_extracted": leadsExtracted,
	}

	tokenStoreBytes, err := json.MarshalIndent(tokenStore, "", "  ")
	if err != nil {
		return fmt.Errorf("erro ao fazer marshal dos tokens: %v", err)
	}

	err = os.WriteFile("/app/lead-search/next_page_tokens.json", tokenStoreBytes, 0644)
	if err != nil {
		return fmt.Errorf("erro ao salvar o arquivo JSON: %v", err)
	}

	log.Println("next_page_token salvo com sucesso")
	return nil
}

func updateSearchProgress(db *sql.DB, progressID int, pagesFetched int, leadsExtracted int, searchDone bool) error {
	searchDoneValue := 0
	if searchDone {
		searchDoneValue = 1
	}

	query := `
        UPDATE search_progress 
        SET pages_fetched = ?, leads_extracted = ?, search_done = ? 
        WHERE id = ?`

	_, err := db.Exec(query, pagesFetched, leadsExtracted, searchDoneValue, progressID)
	if err != nil {
		return fmt.Errorf("Failed to update search progress: %v", err)
	}

	log.Printf("Search progress updated: %d pages, %d leads extracted, done: %v", pagesFetched, leadsExtracted, searchDone)
	return nil
}

func (s *Service) SearchPlaces(query string, location string, radius int, maxPages int) ([]map[string]interface{}, error) {
	client := resty.New()
	url := "https://maps.googleapis.com/maps/api/place/textsearch/json"

	var allPlaces []map[string]interface{}
	queryKey := generateQueryKey(query, location, radius)

	pageToken, pagesFetched, leadsExtracted, err := loadToken(queryKey)
	if err != nil {
		return nil, fmt.Errorf("erro ao carregar next_page_token: %v", err)
	}

	for {
		params := map[string]string{
			"query":    query,
			"location": location,
			"radius":   fmt.Sprintf("%d", radius),
			"key":      s.APIKey,
		}
		if pageToken != "" {
			params["pagetoken"] = pageToken
		}

		resp, err := client.R().
			SetQueryParams(params).
			Get(url)

		if err != nil {
			return nil, fmt.Errorf("error connecting to Google Places API: %v", err)
		}

		if resp.IsSuccess() {
			var result struct {
				Results       []PlaceResult `json:"results"`
				Status        string        `json:"status"`
				ErrorMessage  string        `json:"error_message"`
				NextPageToken string        `json:"next_page_token"`
			}

			err := json.Unmarshal(resp.Body(), &result)
			if err != nil {
				return nil, fmt.Errorf("error parsing response: %v", err)
			}

			if result.Status == "ZERO_RESULTS" {
				log.Printf("Nenhum resultado encontrado para a consulta: %s", query)
				break
			} else if result.Status != "OK" {
				return nil, fmt.Errorf("API error: %s, message: %s", result.Status, result.ErrorMessage)
			}

			for _, place := range result.Results {
				placeDetails := map[string]interface{}{
					"Name":              place.Name,
					"FormattedAddress":  place.FormattedAddress,
					"PlaceID":           place.PlaceID,
					"Rating":            place.Rating,
					"UserRatingsTotal":  place.UserRatingsTotal,
					"PriceLevel":        place.PriceLevel,
					"BusinessStatus":    place.BusinessStatus,
					"Vicinity":          place.Vicinity,
					"PermanentlyClosed": place.PermanentlyClosed,
					"Types":             place.Types,
				}
				allPlaces = append(allPlaces, placeDetails)
				leadsExtracted++
			}

			pagesFetched++
			log.Printf("Página %d obtida, total de resultados até agora: %d", pagesFetched, leadsExtracted)

			saveToken(queryKey, result.NextPageToken, pagesFetched, leadsExtracted)

			if result.NextPageToken == "" || pagesFetched >= maxPages {
				break
			}

			pageToken = result.NextPageToken
			time.Sleep(2 * time.Second)
		} else {
			return nil, fmt.Errorf("failed to get data: %v", resp.Status())
		}
	}

	log.Printf("Total de resultados obtidos: %d", leadsExtracted)
	return allPlaces, nil
}

func SaveProgressToDB(db *sql.DB, query string, location string, radius int, pagesFetched int, leadsExtracted int, token string) error {
	log.Println("saveProgressToDB: Entrando na função")
	log.Printf("saveProgressToDB: query='%s', location='%s', radius=%d, pagesFetched=%d, leadsExtracted=%d, token='%s'\n",
		query, location, radius, pagesFetched, leadsExtracted, token)

	// Verifica se já existe um registro para a consulta
	var exists bool
	queryStr := `SELECT EXISTS(SELECT 1 FROM query_progress WHERE query=? AND location=? AND radius=?)`
	err := db.QueryRow(queryStr, query, location, radius).Scan(&exists)
	if err != nil {
		log.Printf("Erro ao verificar a existência do registro: %v\n", err)
		return err
	}

	if exists {
		// Atualiza o progresso existente
		log.Println("saveProgressToDB: Atualizando registro existente.")
		updateStr := `UPDATE query_progress SET pages_fetched=?, leads_extracted=?, next_page_token=? WHERE query=? AND location=? AND radius=?`
		result, err := db.Exec(updateStr, pagesFetched, leadsExtracted, token, query, location, radius)
		if err != nil {
			log.Printf("Erro ao atualizar o registro: %v\n", err)
			return err
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			log.Printf("Erro ao obter linhas afetadas na atualização: %v\n", err)
			return err
		}
		if rowsAffected == 0 {
			log.Printf("Aviso: Nenhuma linha foi atualizada para query='%s', location='%s', radius=%d\n", query, location, radius)
		} else {
			log.Printf("Sucesso: %d linha(s) foram atualizadas para query='%s', location='%s', radius=%d\n", rowsAffected, query, location, radius)
		}
	} else {
		// Insere um novo progresso
		log.Println("saveProgressToDB: Inserindo novo registro.")
		insertStr := `INSERT INTO query_progress (query, location, radius, pages_fetched, leads_extracted, next_page_token) VALUES (?, ?, ?, ?, ?, ?)`
		result, err := db.Exec(insertStr, query, location, radius, pagesFetched, leadsExtracted, token)
		if err != nil {
			log.Printf("Erro ao inserir o registro: %v\n", err)
			return err
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			log.Printf("Erro ao obter linhas afetadas na inserção: %v\n", err)
			return err
		}
		if rowsAffected == 0 {
			log.Printf("Aviso: Nenhuma linha foi inserida para query='%s', location='%s', radius=%d\n", query, location, radius)
		} else {
			log.Printf("Sucesso: %d linha(s) foram inseridas para query='%s', location='%s', radius=%d\n", rowsAffected, query, location, radius)
		}
	}

	return nil
}

func (s *Service) GetPlaceDetails(placeID string) (map[string]interface{}, error) {
	client := resty.New()

	url := "https://maps.googleapis.com/maps/api/place/details/json"
	resp, err := client.R().
		SetQueryParams(map[string]string{
			"place_id": placeID,
			"key":      s.APIKey,
			"fields":   "name,formatted_address,international_phone_number,website,rating,address_components,editorial_summary",
		}).
		Get(url)

	if err != nil {
		return nil, fmt.Errorf("error connecting to Google Places Details API: %v", err)
	}

	if resp.IsSuccess() {
		var result struct {
			Result struct {
				Name                     string  `json:"name"`
				FormattedAddress         string  `json:"formatted_address"`
				InternationalPhoneNumber string  `json:"international_phone_number"`
				Website                  string  `json:"website"`
				Rating                   float64 `json:"rating"`
				AddressComponents        []struct {
					LongName  string   `json:"long_name"`
					ShortName string   `json:"short_name"`
					Types     []string `json:"types"`
				} `json:"address_components"`
				EditorialSummary struct {
					Overview string `json:"overview"`
				} `json:"editorial_summary"`
			} `json:"result"`
			Status       string `json:"status"`
			ErrorMessage string `json:"error_message"`
		}

		err := json.Unmarshal(resp.Body(), &result)
		if err != nil {
			return nil, fmt.Errorf("error parsing place details response: %v", err)
		}

		if result.Status != "OK" {
			return nil, fmt.Errorf("error from API: %s, message: %s", result.Status, result.ErrorMessage)
		}

		var city, state, zipCode, country, route, neighborhood, streetNumber string
		for _, component := range result.Result.AddressComponents {
			for _, ctype := range component.Types {
				switch ctype {
				case "locality":
					city = component.LongName
				case "administrative_area_level_1":
					state = component.ShortName
				case "postal_code":
					zipCode = component.LongName
				case "country":
					country = component.LongName
				case "street_number":
					streetNumber = component.LongName
				case "route":
					route = component.LongName
				case "neighborhood", "sublocality", "sublocality_level_1", "sublocality_level_2", "administrative_area_level_2":
					if neighborhood == "" {
						neighborhood = component.LongName
					}

				}
			}
		}

		addressParts := []string{}
		if route != "" {
			addressParts = append(addressParts, route)
		}
		if streetNumber != "" {
			addressParts = append(addressParts, streetNumber)
		}
		if neighborhood != "" {
			addressParts = append(addressParts, neighborhood)
		}
		address := strings.Join(addressParts, ", ")

		var description string
		if result.Result.EditorialSummary.Overview != "" {
			description = fmt.Sprintf("(Google Places: %s)", result.Result.EditorialSummary.Overview)
		} else {
			description = "(Google Places: No description available)"
		}

		log.Printf("Address components included: %v", addressParts)

		return map[string]interface{}{
			"Name":                     result.Result.Name,
			"FormattedAddress":         address,
			"InternationalPhoneNumber": result.Result.InternationalPhoneNumber,
			"Website":                  result.Result.Website,
			"Rating":                   result.Result.Rating,
			"City":                     city,
			"State":                    state,
			"ZIPCode":                  zipCode,
			"Country":                  country,
			"PlaceID":                  placeID,
			"Description":              description,
		}, nil
	}

	return nil, fmt.Errorf("failed to get place details: %v", resp.Status())
}
