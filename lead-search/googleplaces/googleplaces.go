package googleplaces

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
)

type Service struct {
	APIKey string
}



type PlaceResult struct {
    Name             string    `json:"name"`
    
    FormattedAddress string    `json:"formatted_address"`
    PlaceID          string    `json:"place_id"`
    Rating           float64   `json:"rating"`
    UserRatingsTotal int       `json:"user_ratings_total"`
    PriceLevel       int       `json:"price_level"`
    BusinessStatus   string    `json:"business_status"`
    Vicinity         string    `json:"vicinity"`
    PermanentlyClosed bool     `json:"permanently_closed"`
    Types            []string  `json:"types"`
 
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


func saveNextPageToken(token string) error {
	
	err := os.WriteFile("/app/lead-search/next_page_token.txt", []byte(token), 0644)

	if err != nil {
		log.Printf("Erro ao salvar o next_page_token: %v", err)
		return err
	}
	log.Println("next_page_token salvo com sucesso")
	return nil
}

func LoadNextPageToken() (string, error) {
	file, err := os.Open("next_page_token.txt")
	if err != nil {
		log.Printf("Erro ao abrir o arquivo next_page_token: %v", err)
		return "", err
	}
	defer file.Close()

	tokenBytes, err := io.ReadAll(file)
	if err != nil {
		return "", err
	}
	token := string(tokenBytes)
	log.Printf("next_page_token carregado: %s", token)
	return token, nil
}



func (s *Service) SearchPlaces(query string, location string, radius int, maxPages int) ([]map[string]interface{}, error) {
    client := resty.New()
    url := "https://maps.googleapis.com/maps/api/place/textsearch/json"

    var allPlaces []map[string]interface{}
    pageToken, err := LoadNextPageToken()  
	if err != nil {
		return nil, fmt.Errorf("erro ao carregar next_page_token: %v", err)
	}
    pagesFetched := 0
	totalResults := 0

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
                Results           []PlaceResult `json:"results"`
                Status            string        `json:"status"`
                ErrorMessage      string        `json:"error_message"`
                NextPageToken     string        `json:"next_page_token"`
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
            }

			totalResults += len(result.Results)
            pagesFetched++
			
			log.Printf("Página %d obtida, total de resultados até agora: %d", pagesFetched, totalResults)
            if result.NextPageToken != "" {
				err := saveNextPageToken(result.NextPageToken)
				if err != nil {
					log.Printf("Erro ao salvar o next_page_token: %v", err)
				}
			} else {
				
				saveNextPageToken("")
				break
			}

            if pagesFetched >= maxPages {
				break
			}

            
            time.Sleep(2 * time.Second)

            pageToken = result.NextPageToken
        } else {
            return nil, fmt.Errorf("failed to get data: %v", resp.Status())
        }
    }

	log.Printf("Total de resultados obtidos: %d", totalResults)
    return allPlaces, nil
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
				Name        string `json:"name"`
				FormattedAddress string `json:"formatted_address"`
				InternationalPhoneNumber string `json:"international_phone_number"`
				Website     string `json:"website"`
				Rating      float64 `json:"rating"`
				AddressComponents         []struct {
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
        }


		log.Printf("Address components included: %v", addressParts)


		return map[string]interface{}{
			"Name":                      result.Result.Name,
			"FormattedAddress":          address,
			"InternationalPhoneNumber":  result.Result.InternationalPhoneNumber,
			"Website":                   result.Result.Website,
			"Rating":                    result.Result.Rating,
			"City":                      city,
            "State":                     state,
            "ZIPCode":                   zipCode,
			"Country":                  country,
            "PlaceID":                   placeID,
			"Description":              description,
			
			
		}, nil
	}

	return nil, fmt.Errorf("failed to get place details: %v", resp.Status())
}

