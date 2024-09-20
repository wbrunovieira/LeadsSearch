package googleplaces

import (
	"encoding/json"
	"fmt"

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

func (s *Service) SearchPlaces(query string, location string, radius int) ([]string, error) {
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
					Viewport struct {
						Northeast struct {
							Lat float64 `json:"lat"`
							Lng float64 `json:"lng"`
						} `json:"northeast"`
						Southwest struct {
							Lat float64 `json:"lat"`
							Lng float64 `json:"lng"`
						} `json:"southwest"`
					} `json:"viewport"`
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

		fmt.Println("Places found:")
		var placeIDs []string
		for _, place := range result.Results {
			fmt.Printf("Name: %s\nAddress: %s\nRating: %.1f\nPlace ID: %s\nUser Ratings Total: %d\nPrice Level: %d\nBusiness Status: %s\nIcon: %s\nVicinity: %s\n\n",
				place.Name, place.Address, place.Rating, place.PlaceID, place.UserRatingsTotal, place.PriceLevel, place.BusinessStatus, place.Icon, place.Vicinity)
			
			
			if len(place.Photos) > 0 {
				for _, photo := range place.Photos {
					fmt.Printf("Photo Reference: %s (Size: %dx%d)\n", photo.PhotoReference, photo.Width, photo.Height)
				}
			}

			if len(place.Types) > 0 {
				fmt.Printf("Types: %v\n", place.Types)
			}

			if place.PlusCode.CompoundCode != "" {
				fmt.Printf("Plus Code: %s\n", place.PlusCode.CompoundCode)
			}

			if place.PermanentlyClosed {
				fmt.Println("This place is permanently closed.")
			}

			placeIDs = append(placeIDs, place.PlaceID)
			fmt.Println()
		}
		return placeIDs, nil
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
				Reviews     []struct {
					AuthorName  string `json:"author_name"`
					Rating      int    `json:"rating"`
					Text        string `json:"text"`
				} `json:"reviews"`
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
			"Reviews":                   result.Result.Reviews,
		}, nil
	}

	return nil, fmt.Errorf("failed to get place details: %v", resp.Status())
}
