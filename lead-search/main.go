package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"lead-search/googleplaces"

	"github.com/streadway/amqp"

	"github.com/joho/godotenv"
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

    if apiKey ==  "" {
        log.Fatal("API key is required. Set the GOOGLE_PLACES_API_KEY environment variable.")
    }

    service := googleplaces.NewService(apiKey)
    city := "Campinas"
    categoria := "restaurantes"
    radius:= 10000

    coordinates, err := service.GeocodeCity(city)
    if err != nil {
        log.Fatalf("Failed to get coordinates for city: %v", err)
    }
    maxPages := 1
    placeDetailsFromSearch, err := service.SearchPlaces(categoria, coordinates, radius, maxPages)
    if err != nil {
        log.Fatalf("Failed to search places: %v", err)
    }

    for _, place := range placeDetailsFromSearch {
        placeID := place["PlaceID"].(string)
        placeDetails, err := service.GetPlaceDetails(placeID)
        if err != nil {
            log.Printf("Failed to get details for place ID %s: %v", placeID, err)
            continue

        }

        for k, v := range place {
            placeDetails[k] = v
        }

        placeDetails["Category"] = categoria
        placeDetails["City"] = city
        placeDetails["Radius"] = radius
    
        
        err = publishLeadToRabbitMQ(ch, placeDetails)
        if err != nil {
            log.Printf("Failed to publish lead to RabbitMQ: %v", err)
        }
    }
    
   
}
func publishLeadToRabbitMQ(ch *amqp.Channel, leadData map[string]interface{}) error {
    exchangeName := "leads_exchange"

   

    err := ch.ExchangeDeclare(
        exchangeName, // nome do exchange
        "fanout",     // tipo
        true,         // durável
        false,        // auto-deletar
        false,        // interno
        false,        // sem espera
        nil,          // argumentos
    )
    if err != nil {
        return fmt.Errorf("Failed to declare exchange: %v", err)
    }
  
    // Serialize lead data
    body, err := json.Marshal(leadData)
    if err != nil {
        return fmt.Errorf("Failed to serialize lead data: %v", err)
    }

    // Publish the message
    err = ch.Publish(
        exchangeName,
        "",
        false,
        false,
        amqp.Publishing{
            ContentType: "application/json",
            Body:        body,
        },
    )
    if err != nil {
        return fmt.Errorf("Failed to publish a message: %v", err)
    }

    log.Printf("Lead published to RabbitMQ: %s", body)
    return nil
}


func connectToRabbitMQ() (*amqp.Connection, error) {
    log.Println("Conectando ao RabbitMQ...")
    rabbitmqHost := os.Getenv("RABBITMQ_HOST")
    rabbitmqPort := os.Getenv("RABBITMQ_PORT")
    if rabbitmqHost ==  "" || rabbitmqPort == ""{
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
        "" ,     
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