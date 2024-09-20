package main

import (
	"fmt"
	"lead-search/googleplaces"
	"lead-search/cnpjsearch"
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

    city:= "Osasco"
    categoria:= "restaurantes"

    coordinates, err := service.GeocodeCity(city)
	if err != nil {
		log.Fatalf("Failed to get coordinates for city: %v", err)
	}

         

    // gratuito 7945 por mes a custo de 100 por requisicao cerca de 260 por dia, considerando 30 dias

   

    placeIDs, err := service.SearchPlaces(categoria, coordinates, 1000)
	if err != nil {
		log.Fatalf("Failed to search places: %v", err)
	}

    for _, placeID := range placeIDs {
		details, err := service.GetPlaceDetails(placeID)
		if err != nil {
			log.Printf("Failed to get details for place ID %s: %v", placeID, err)
			continue
		}
		name := details["Name"].(string)
        fmt.Printf("Details for Place ID %s:\n", placeID)
		fmt.Printf("Name: %s\n", details["Name"])
		fmt.Printf("Address: %s\n", details["FormattedAddress"])
		fmt.Printf("Phone: %s\n", details["InternationalPhoneNumber"])
		fmt.Printf("Website: %s\n", details["Website"])
		fmt.Printf("Rating: %.1f\n", details["Rating"])

		cnpjsearch.FetchData(name,city)

		
		cnpj := "31301298000101" 
		
		cnpjsearch.FetchCompanyDetails(cnpj)


		fmt.Println()
	}
}