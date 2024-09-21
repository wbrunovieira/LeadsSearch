package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"lead-search/googleplaces"

	"github.com/joho/godotenv"
	"github.com/wbrunovieira/ProtoDefinitionsLeadsSearch/leadpb"
	"google.golang.org/grpc"

	"google.golang.org/grpc/credentials/insecure"
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
        placeDetailsFromDetails, err := service.GetPlaceDetails(placeID)
        if err != nil {
            log.Printf("Failed to get details for place ID %s: %v", placeID, err)
            continue
        }

       
        sendLeadToAPI(placeDetailsFromDetails)
    }
}

func sendLeadToAPI(details map[string]interface{}) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

     conn, err := grpc.DialContext(ctx, "api:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))

    if err != nil {
        log.Fatalf("Failed to connect to API service: %v", err)
    }
    defer conn.Close()

   

    client := leadpb.NewLeadServiceClient(conn)

    req := &leadpb.LeadRequest{
        BusinessName:         details["Name"].(string),
        RegisteredName:       details["Name"].(string), 
        Address:              details["FormattedAddress"].(string),
        City:                 "Osasco", 
        State:                "SP",
        Country:              "Brazil",
        ZipCode:              details["ZIPCode"].(string), 
        Owner:                "", 
        Phone:                details["InternationalPhoneNumber"].(string),
        Whatsapp:             "", 
        Website:              details["Website"].(string),
        Email:                "", 
        Instagram:            details["Instagram"].(string),
        Facebook:             "", 
        Tiktok:               "", 
        CompanyRegistrationId: "", 
        Rating:               float32(details["Rating"].(float64)),
        PriceLevel:           float32(details["PriceLevel"].(float64)),
        UserRatingsTotal:      int32(details["UserRatingsTotal"].(int)),
        FoundationDate:       "",
    }


    res, err := client.ReceiveLead(ctx, req)
    
    if err != nil {
        log.Fatalf("Error while sending lead: %v", err)
    }

    fmt.Printf("Response from API: %s\n", res.GetMessage())
}
