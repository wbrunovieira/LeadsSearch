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

    // gratuito 7945 por mes a custo de 100 por requisicao cerca de 260 por dia, considerando 30 dias

    service.SearchPlaces("restaurants", "40.748817,-73.985428", 1000)
}
