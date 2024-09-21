package main

import (
	"fmt"
	"lead-search/googleplaces"

	"log"
	"os"

	"github.com/joho/godotenv"
)

func main() {
    err := godotenv.Load()
    if err != nil {
        log.Fatal("Error loading .env file")
    }

    apiKey := os.Getenv("GOOGLE_PLACES_API_KEY")
    if apiKey == "" {
        log.Fatal("API key is required. Set the GOOGLE_PLACES_API_KEY environment variable.")
    }

    service := googleplaces.NewService(apiKey)

    city := "Osasco"
    categoria := "restaurantes"

    coordinates, err := service.GeocodeCity(city)
    if err != nil {
        log.Fatalf("Failed to get coordinates for city: %v", err)
    }

    placeDetailsFromSearch, err := service.SearchPlaces(categoria, coordinates, 1000)
    if err != nil {
        log.Fatalf("Failed to search places: %v", err)
    }

    for _, place := range placeDetailsFromSearch {
        placeID := place["PlaceID"].(string)

       
        fmt.Printf("SearchPlaces result for Place ID %s:\n", placeID)
        for k, v := range place {
            fmt.Printf("%s: %v\n", k, v)
        }
        fmt.Println()

        
        placeDetailsFromDetails, err := service.GetPlaceDetails(placeID)
        if err != nil {
            log.Printf("Failed to get details for place ID %s: %v", placeID, err)
            continue
        }

       
        fmt.Printf("GetPlaceDetails result for Place ID %s:\n", placeID)
        for k, v := range placeDetailsFromDetails {
            fmt.Printf("%s: %v\n", k, v)
        }
        fmt.Println()

       
        combinedDetails := make(map[string]interface{})
        for k, v := range place {
            combinedDetails[k] = v
        }
        for k, v := range placeDetailsFromDetails {
            combinedDetails[k] = v
        }

       
        fmt.Printf("Combined Details for Place ID %s:\n", placeID)
        for k, v := range combinedDetails {
            fmt.Printf("%s: %v\n", k, v)
        }
        fmt.Println()
    }
}

