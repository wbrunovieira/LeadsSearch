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

    businessName, ok := details["Name"].(string)
    if !ok {
        businessName = "" 
    }

    registeredName := businessName 

    address, ok := details["FormattedAddress"].(string)
    if !ok {
        address = ""
    }

    zipcode, ok := details["ZIPCode"].(string)
    if !ok {
        zipcode = ""
    }

    phone, ok := details["InternationalPhoneNumber"].(string)
    if !ok {
        phone = ""
    }

    website, ok := details["Website"].(string)
    if !ok {
        website = ""
    }

    email, ok := details["Email"].(string)
    if !ok {
        email = ""
    }

    instagram, ok := details["Instagram"].(string)
    if !ok {
        instagram = ""
    }

    facebook, ok := details["Facebook"].(string)
    if !ok {
        facebook = ""
    }

    rating, ok := details["Rating"].(float64)
    if !ok {
        rating = 0.0
    }

    priceLevel, ok := details["PriceLevel"].(float64)
    if !ok {
        priceLevel = 0.0
    }

    userRatingsTotal, ok := details["UserRatingsTotal"].(int)
    if !ok {
        userRatingsTotal = 0
    }

    req := &leadpb.LeadRequest{
        BusinessName:         businessName,
        RegisteredName:       registeredName,
        Address:              address,
        City:                 "Osasco", 
        State:                "SP",
        Country:              "Brazil",
        ZipCode:              zipcode,
        Owner:                "", 
        Phone:                phone,
        Whatsapp:             "",
        Website:              website,
        Email:                email,
        Instagram:            instagram,
        Facebook:             facebook,
        Tiktok:               "", 
        CompanyRegistrationId: "", 
        Rating:               float32(rating), 
        PriceLevel:           float32(priceLevel),
        UserRatingsTotal:      int32(userRatingsTotal),
        FoundationDate:       "", 
        Source:               "Google Places", 

    }


    res, err := client.ReceiveLead(ctx, req)
    
    if err != nil {
        log.Fatalf("Error while sending lead: %v", err)
    }

    fmt.Printf("Response from API: %s\n", res.GetMessage())
}
