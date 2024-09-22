package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"lead-search/googleplaces"

	"github.com/streadway/amqp"

	"github.com/joho/godotenv"
	"github.com/wbrunovieira/ProtoDefinitionsLeadsSearch/leadpb"
	"google.golang.org/grpc"

	"google.golang.org/grpc/credentials/insecure"
)

func main() {
    log.Println("Starting the service...")
    err := godotenv.Load()
    if err != nil {
        log.Fatal("Error loading .env file")
    }
    log.Println(".env file loaded successfully")

    conn, err := connectToRabbitMQ()
	if err != nil {
		log.Fatalf("Could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

    ch, err := conn.Channel()
    if err != nil {
        log.Fatalf("Failed to open a channel: %v", err)
    }
    defer ch.Close()

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

       
        err = sendLeadViaGrpc(placeDetailsFromDetails)

        if err != nil {
            log.Printf("gRPC failed, sending to RabbitMQ: %v", err)
            err = sendLeadToRabbitMQ(ch,placeDetailsFromDetails)
            if err != nil {
				log.Printf("Failed to send to RabbitMQ: %v", err)
			}
        }
    }

   



   
}

func connectToRabbitMQ() (*amqp.Connection, error) {
    log.Println("Conectando ao RabbitMQ...")
    rabbitmqHost := os.Getenv("RABBITMQ_HOST")
    rabbitmqPort := os.Getenv("RABBITMQ_PORT")
    if rabbitmqHost == "" || rabbitmqPort == "" {
        return nil, fmt.Errorf("RABBITMQ_HOST and RABBITMQ_PORT must be set")
    }
    log.Printf("Conectado ao RabbitMQ em %s:%s", rabbitmqHost, rabbitmqPort)

	var conn *amqp.Connection
    var err error

	for i := 0; i < 5; i++ {
        conn, err = amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%s/", rabbitmqHost, rabbitmqPort))
        if err == nil {
            return conn, nil
        }

        log.Printf("Failed to connect to RabbitMQ at %s:%s, retrying in 10 seconds... (%d/5)", rabbitmqHost, rabbitmqPort, i+1)
        time.Sleep(10 * time.Second)
    }

    return nil, fmt.Errorf("failed to connect to RabbitMQ at %s:%s after 5 retries: %v", rabbitmqHost, rabbitmqPort, err)
}

func sendLeadViaGrpc(details map[string]interface{})error  {
    log.Println("Enviando lead via gRPC...")

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

     conn, err := grpc.DialContext(ctx, "api:8090", grpc.WithTransportCredentials(insecure.NewCredentials()))

    if err != nil {
        log.Fatalf("Failed to connect to API service: %v", err)
    }
    defer conn.Close()

    client := leadpb.NewLeadServiceClient(conn)
    log.Println("Cliente gRPC criado com sucesso.")

    businessName, ok := details["Name"].(string)
    if !ok {
        businessName = "" 
    }

    registeredName := businessName 
    log.Printf("Enviando lead: %v", details)

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
    log.Printf("Resposta da API gRPC: %s", res.GetMessage())

    return nil
}


func sendLeadToRabbitMQ(ch *amqp.Channel, details map[string]interface{}) error {
    log.Println("Iniciando envio de lead via gRPC...")
    q, err := ch.QueueDeclare(
        "leads_queue", 
        false,         
        false,         
        false,         
        false,         
        nil,           
    )
    if err != nil {
        return fmt.Errorf("Failed to declare a queue: %v", err)
    }

    leadData, err := json.Marshal(details)
    if err != nil {
        return fmt.Errorf("Failed to serialize lead data: %v", err)
    }

    err = ch.Publish(
        "",     
        q.Name, 
        false,  
        false,  
        amqp.Publishing{
            ContentType: "application/json",
            Body:        leadData,
        })
    if err != nil {
        log.Println("Tentando enviar lead via RabbitMQ devido à falha no gRPC...")

        return fmt.Errorf("Failed to publish a message to RabbitMQ: %v", err)
    }

    log.Printf("Lead sent to RabbitMQ for later processing: %s", leadData)
    return nil
}