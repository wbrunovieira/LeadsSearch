package main

import (
	"api/db"
	"context"
	"database/sql"
	"encoding/json"
	"github.com/streadway/amqp"
	"fmt"
	"log"
	"net/http"
	"net"
	"os"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/wbrunovieira/ProtoDefinitionsLeadsSearch/leadpb"

	"time"
)

type LeadServer struct {
	leadpb.UnimplementedLeadServiceServer
}

func (s *LeadServer) ReceiveLead(ctx context.Context, req *leadpb.LeadRequest) (*leadpb.LeadResponse, error) {

	lead := db.Lead{
		BusinessName: req.GetBusinessName(),
		RegisteredName: req.GetRegisteredName(),
		Address: req.GetAddress(),
		City: req.GetCity(),
		State: req.GetState(),
		Country: req.GetCountry(),
		ZIPCode: req.GetZipCode(),
		Owner: req.GetOwner(),
		Phone: req.GetPhone(),
		Whatsapp: req.GetWhatsapp(),
		Website: req.GetWebsite(),
		Email: req.GetEmail(),
		Instagram: req.GetInstagram(),
		Facebook: req.GetFacebook(),
		TikTok: req.GetTiktok(),
		CompanyRegistrationID: req.GetCompanyRegistrationId(),
		Rating: float64(req.GetRating()),
		PriceLevel: float64(req.GetPriceLevel()),
		UserRatingsTotal: int(req.GetUserRatingsTotal()),
	}

	if req.GetFoundationDate() == "" {
		lead.FoundationDate = sql.NullTime{Valid: false}
	} else {
		parsedDate, err := time.Parse("2006-01-02", req.GetFoundationDate())
		if err != nil {
			return nil, fmt.Errorf("data inválida: %v", err)
		}
		lead.FoundationDate = sql.NullTime{Time: parsedDate, Valid: true}
	}

	
	err := db.CreateLead(&lead)
	if err != nil {
		return nil, fmt.Errorf("failed to save lead: %v", err)
	}

	return &leadpb.LeadResponse{
		Message: "Lead salvo com sucesso!",
		Success: true,
	}, nil
}


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

func startGrpcServer() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen on port 8080: %v", err)
	}

	grpcServer := grpc.NewServer()
	leadpb.RegisterLeadServiceServer(grpcServer, &LeadServer{})
	reflection.Register(grpcServer)

	log.Println("gRPC server is running on port 8080...")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v", err)
	}
}

// Função para consumir leads da fila RabbitMQ
func consumeLeadsFromRabbitMQ() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"leads_queue", // Nome da fila
		false,         // Não persistente
		false,         // Não deletar quando ocioso
		false,         // Não exclusivo
		false,         // No-wait
		nil,           // Argumentos adicionais
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name, // Nome da fila
		"",     // Nome do consumidor
		true,   // Auto-ack (confirmação automática)
		false,  // Exclusivo
		false,  // No-local
		false,  // No-wait
		nil,    // Argumentos adicionais
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message from RabbitMQ: %s", d.Body)

			// Processar a mensagem recebida (JSON do lead)
			var lead leadpb.LeadRequest
			err := json.Unmarshal(d.Body, &lead)
			if err != nil {
				log.Printf("Failed to unmarshal lead data: %v", err)
				continue
			}

			// Processar o lead (pode ser salvar no banco de dados)
			log.Printf("Processing lead from RabbitMQ: %+v", lead)
		}
	}()

	log.Printf(" [*] Waiting for messages from RabbitMQ. To exit press CTRL+C")
	<-forever
}

func connectToRabbitMQ() (*amqp.Connection, error) {
    var conn *amqp.Connection
    var err error

    for i := 0; i < 5; i++ {
        conn, err = amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
        if err == nil {
            return conn, nil
        }

        log.Printf("Failed to connect to RabbitMQ, retrying in 5 seconds... (%d/5)", i+1)
        time.Sleep(5 * time.Second)
    }

    return nil, fmt.Errorf("failed to connect to RabbitMQ after 5 retries: %v", err)
}

func main() {

	go startGrpcServer() 

	conn, err := connectToRabbitMQ()
	if err != nil {
		log.Fatalf("Could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	consumeLeadsFromRabbitMQ() 
	
	db.Connect()
	defer db.Close()

	
	db.Migrate()

	
	http.HandleFunc("/leads", leadHandler)

	
	port := os.Getenv("PORT")
	fmt.Println("API rodando na porta", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
