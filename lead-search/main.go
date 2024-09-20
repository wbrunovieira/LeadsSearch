package main

import (
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

    city:= "Osasco"
    categoria:= "restaurantes"

    coordinates, err := service.GeocodeCity(city)
	if err != nil {
		log.Fatalf("Failed to get coordinates for city: %v", err)
	}

         

    // gratuito 7945 por mes a custo de 100 por requisicao cerca de 260 por dia, considerando 30 dias

    service.SearchPlaces(categoria, coordinates, 1000)
}
