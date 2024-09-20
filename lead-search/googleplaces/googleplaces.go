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
	client := resty.New()

	geocodeURL := "https://maps.googleapis.com/maps/api/geocode/json"
	resp, err := client.R().
		SetQueryParams(map[string]string{
			"address": city,
			"key":     s.APIKey,
		}).
		Get(geocodeURL)

	if err != nil {
		return "", fmt.Errorf("Error connecting to Geocoding API: %v", err)
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
		return "", fmt.Errorf("Error parsing geocode response: %v", err)
	}

	if result.Status != "OK" {
		return "", fmt.Errorf("Geocoding API error: %s, message: %s", result.Status, result.ErrorMessage)
	}

	
	if len(result.Results) > 0 {
		lat := result.Results[0].Geometry.Location.Lat
		lng := result.Results[0].Geometry.Location.Lng
		return fmt.Sprintf("%f,%f", lat, lng), nil
	}

	return "", fmt.Errorf("No results found for city: %s", city)
}


func (s *Service) SearchPlaces(query string, location string, radius int) {
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
        log.Fatalf("Error connecting to Google Places API: %v", err)
    }

    if resp.IsSuccess() {
        
        var result struct {
            Results []struct {
                Name    string `json:"name"`
                Address string `json:"formatted_address"`
                Rating  float64 `json:"rating"`
            } `json:"results"`
            Status string `json:"status"`
            ErrorMessage string `json:"error_message"`
        }

       
        err := json.Unmarshal(resp.Body(), &result)
        if err != nil {
            log.Fatalf("Error parsing response: %v", err)
        }
        
        if result.Status != "OK" {
            log.Fatalf("Error from API: %s, message: %s", result.Status, result.ErrorMessage)
        } else {
        
        fmt.Println("Places found:")
        for _, place := range result.Results {
            fmt.Printf("Name: %s\nAddress: %s\nRating: %.1f\n\n", place.Name, place.Address, place.Rating)
        } }
    } else {
        fmt.Printf("Failed to get data: %v\n", resp.Status())
    }
}
