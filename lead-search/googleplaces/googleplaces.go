package googleplaces

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/go-resty/resty/v2"
)

type Service struct {
	APIKey string
}

func NewService(apiKey string) *Service {
	return &Service{APIKey: apiKey}
}

func (s *Service) GeocodeCity(city string) (string, error) {
	log.Printf("Buscando coordenadas para a cidade: %s", city)

	client := resty.New()

	geocodeURL := "https://maps.googleapis.com/maps/api/geocode/json"
	resp, err := client.R().
		SetQueryParams(map[string]string{
			"address": city,
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

	return "", fmt.Errorf("no results found for city: %s", city)
}

func (s *Service) SearchPlaces(query string, location string, radius int) ([]map[string]interface{}, error) {

	client := resty.New()

	url := "https://maps.googleapis.com/maps/api/place/textsearch/json"
	resp, err := client.R().
		SetQueryParams(map[string]string{
			"query":    query,
			"location": location,
			"radius":   fmt.Sprintf("%d", radius),
			"key":      s.APIKey,
		}).
		Get(url)
		

	if err != nil {
		return nil, fmt.Errorf("error connecting to Google Places API: %v", err)
	}

	if resp.IsSuccess() {
		var result struct {
			Results []struct {
				Name             string  `json:"name"`
				Address          string  `json:"formatted_address"`
				PlaceID          string  `json:"place_id"`
				Rating           float64 `json:"rating"`
				UserRatingsTotal int     `json:"user_ratings_total"`
				PriceLevel       int     `json:"price_level"`
				BusinessStatus   string  `json:"business_status"`
				OpeningHours     struct {
					OpenNow bool `json:"open_now"`
				} `json:"opening_hours"`
				Photos []struct {
					PhotoReference   string   `json:"photo_reference"`
					Height           int      `json:"height"`
					Width            int      `json:"width"`
					HtmlAttributions []string `json:"html_attributions"`
				} `json:"photos"`
				Geometry struct {
					Location struct {
						Lat float64 `json:"lat"`
						Lng float64 `json:"lng"`
					} `json:"location"`
				} `json:"geometry"`
				Icon              string   `json:"icon"`
				Vicinity          string   `json:"vicinity"`
				PermanentlyClosed bool     `json:"permanently_closed"`
				Types             []string `json:"types"`
				PlusCode          struct {
					CompoundCode string `json:"compound_code"`
					GlobalCode   string `json:"global_code"`
				} `json:"plus_code"`
			} `json:"results"`
			Status       string `json:"status"`
			ErrorMessage string `json:"error_message"`
		}

		err := json.Unmarshal(resp.Body(), &result)
		if err != nil {
			return nil, fmt.Errorf("error parsing response: %v", err)
		}

		if result.Status != "OK" {
			return nil, fmt.Errorf("error from API: %s, message: %s", result.Status, result.ErrorMessage)
		}

		places := []map[string]interface{}{}
		for _, place := range result.Results {
			placeDetails := map[string]interface{}{
				"Name":              place.Name,
				"FormattedAddress":  place.Address,
				"PlaceID":           place.PlaceID,
				"Rating":            place.Rating,
				"UserRatingsTotal":   place.UserRatingsTotal,
				"PriceLevel":        place.PriceLevel,
				"BusinessStatus":    place.BusinessStatus,
				"Vicinity":          place.Vicinity,
				"PermanentlyClosed": place.PermanentlyClosed,
				"Types":             place.Types,
			}
			places = append(places, placeDetails)
		}
		return places, nil
	} else {
		return nil, fmt.Errorf("failed to get data: %v", resp.Status())
	}
}


func (s *Service) GetPlaceDetails(placeID string) (map[string]interface{}, error) {
	client := resty.New()

	url := "https://maps.googleapis.com/maps/api/place/details/json"
	resp, err := client.R().
		SetQueryParams(map[string]string{
			"place_id": placeID,
			"key":      s.APIKey,
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

		return map[string]interface{}{
			"Name":                      result.Result.Name,
			"FormattedAddress":          result.Result.FormattedAddress,
			"InternationalPhoneNumber":  result.Result.InternationalPhoneNumber,
			"Website":                   result.Result.Website,
			"Rating":                    result.Result.Rating,
			
			
		}, nil
	}

	return nil, fmt.Errorf("failed to get place details: %v", resp.Status())
}

