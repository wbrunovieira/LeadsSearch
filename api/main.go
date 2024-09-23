package main

import (
	"api/db"

	"encoding/json"
	"fmt"
	"log"
	"github.com/google/uuid"


	"net/http"
	"os"

	"github.com/streadway/amqp"

	"time"
)






func leadHandler(w http.ResponseWriter, r *http.Request) {
	var lead db.Lead
	err := json.NewDecoder(r.Body).Decode(&lead)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = db.CreateLead(&lead)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "Lead salvo com sucesso!")


}

func connectToRabbitMQ() (*amqp.Connection, error) {
    rabbitmqHost := os.Getenv("RABBITMQ_HOST")
    rabbitmqPort := os.Getenv("RABBITMQ_PORT")
    if rabbitmqHost == "" || rabbitmqPort == "" {
        return nil, fmt.Errorf("RABBITMQ_HOST and RABBITMQ_PORT must be set")
    }

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

func consumeLeadsFromRabbitMQ(ch *amqp.Channel) {
    queueName := "leads_queue"

  q, err := ch.QueueDeclare(
        queueName,
        false, // Durable
        false, // Delete when unused
        false, // Exclusive
        false, // No-wait
        nil,   // Arguments
    )
    if err != nil {
        log.Fatalf("Failed to register a consumer: %v", err)
    }

 msgs, err := ch.Consume(
        q.Name,
        "",
        true,  // Auto-acknowledge
        false, // Exclusive
        false, // No-local
        false, // No-wait
        nil,   // Arguments
    )
    if err != nil {
        log.Fatalf("Failed to register a consumer: %v", err)
    }

    go func() {
        for d := range msgs {
            var leadData map[string]interface{}
            err := json.Unmarshal(d.Body, &leadData)
            if err != nil {
                log.Printf("Error decoding JSON: %v", err)
                continue
            }

            // Save lead to database
            err = saveLeadToDatabase(leadData)
            if err != nil {
                log.Printf("Failed to save lead: %v", err)
            }
        }
    }()

    log.Println("Consuming leads from RabbitMQ...")
    select {} // Block forever
}

func saveLeadToDatabase(data map[string]interface{}) error {
    lead := db.Lead{}

	if lead.ID == uuid.Nil {
        lead.ID = uuid.New()
    }

    if v, ok := data["Name"].(string); ok {
        lead.BusinessName = v
    }
    if v, ok := data["FormattedAddress"].(string); ok {
        lead.Address = v
    }
    if v, ok := data["InternationalPhoneNumber"].(string); ok {
        lead.Phone = v
    }
    if v, ok := data["Website"].(string); ok {
        lead.Website = v
    }
    if v, ok := data["Rating"].(float64); ok {
        lead.Rating = v
    }
    if v, ok := data["UserRatingsTotal"].(float64); ok {
        lead.UserRatingsTotal = int(v)
    }
    if v, ok := data["PriceLevel"].(float64); ok {
        lead.PriceLevel = int(v)
    }
    if v, ok := data["BusinessStatus"].(string); ok {
        lead.BusinessStatus = v
    }
    if v, ok := data["Vicinity"].(string); ok {
        lead.Vicinity = v
    }
    if v, ok := data["PermanentlyClosed"].(bool); ok {
        lead.PermanentlyClosed = v
    }
    if v, ok := data["Types"].([]interface{}); ok {
        typesBytes, err := json.Marshal(v)
        if err == nil {
            lead.Types = string(typesBytes)
        } else {
            log.Printf("Error marshalling types: %v", err)
        }
    }

    // Save lead to the database
    err := db.CreateLead(&lead)
    if err != nil {
        return fmt.Errorf("Failed to save lead to database: %v", err)
    }

    log.Printf("Lead saved to database: %v", lead)
    return nil
}

func main() {

	log.Println("Starting API service...")

	conn, err := connectToRabbitMQ()
	if err != nil {
		log.Fatalf("Could not connect to RabbitMQ: %v", err)
	}
	log.Printf("Successfully connected to RabbitMQ at %s", conn.LocalAddr())
	defer func() {
		log.Println("Closing RabbitMQ connection...")
		conn.Close()
	}()

	channel, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a RabbitMQ channel: %v", err)
	}
	log.Println("Successfully opened a RabbitMQ channel")
defer func() {
    log.Println("Closing RabbitMQ channel...")
    channel.Close()
}()


	log.Println("Starting to consume leads from RabbitMQ...")
	
	
	log.Println("Connecting to the database...")
	db.Connect()

	defer db.Close()
	db.Migrate(db.DB)
	

	
	http.HandleFunc("/leads", leadHandler)

	consumeLeadsFromRabbitMQ(channel)

	port := os.Getenv("PORT")

	fmt.Println("API rodando na porta", port)
	if port == "" {
		log.Fatal("PORT não definido no ambiente")
	} else {
		fmt.Println("API rodando na porta", port)
	}
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
