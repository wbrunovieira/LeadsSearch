package main

import (
	"api/db"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/streadway/amqp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/wbrunovieira/ProtoDefinitionsLeadsSearch/leadpb"

	"time"
)

type LeadServer struct {
	leadpb.UnimplementedLeadServiceServer
	channel *amqp.Channel
}

func (s *LeadServer) SendLead(ctx context.Context, req *leadpb.LeadRequest) (*leadpb.LeadResponse, error) {

	log.Printf("Recebendo lead: %v", req)

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
		log.Println("Data de fundação não fornecida.")
	} else {
		parsedDate, err := time.Parse("2006-01-02", req.GetFoundationDate())
		if err != nil {
			return nil, fmt.Errorf("data inválida: %v", err)
		}
		lead.FoundationDate = sql.NullTime{Time: parsedDate, Valid: true}
		log.Printf("Data de fundação parseada: %v", parsedDate)
	}

	
	err := db.CreateLead(&lead)
	if err != nil {
		log.Printf("Erro ao salvar o lead no banco de dados: %v", err)
		return nil, fmt.Errorf("failed to save lead: %v", err)
	}

	leadData, err := json.Marshal(req)
    if err != nil {
		log.Printf("Erro ao converter o lead para JSON: %v", err)
        return nil, fmt.Errorf("failed to marshal lead: %v", err)
    }

	err = s.channel.Publish(
        "",                     // Exchange
        "leads_queue",          // Routing key
        false,                  // Mandatory
        false,                  // Immediate
        amqp.Publishing{
            ContentType: "application/json",
            Body:        leadData,
        },
    )
    if err != nil {
		log.Printf("Erro ao publicar o lead no RabbitMQ: %v", err)
        return nil, fmt.Errorf("failed to publish lead to RabbitMQ: %v", err)
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

func startGrpcServer(channel *amqp.Channel) {
	log.Println("chamou a startGrpcServer")

	grpcServer := grpc.NewServer()
	log.Println("Iniciando servidor gRPC na porta 8090...")
	leadServer := &LeadServer{channel: channel}
	log.Println("Registrando o serviço LeadService no servidor gRPC...")
	leadpb.RegisterLeadServiceServer(grpcServer, leadServer)
	log.Println("Serviço LeadService registrado com sucesso.")
	reflection.Register(grpcServer)

	listener, err := net.Listen("tcp", ":8090")
	
	if err != nil {
		log.Fatalf("Falha ao iniciar o listener: %v", err)
	}


	log.Println("gRPC server is running on port 8090...")

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v", err)
	}
}


func consumeLeadsFromRabbitMQ(channel *amqp.Channel) {

	q, err := channel.QueueDeclare(
		"leads_queue", 
		false,         
		false,         
		false,         
		false,         
		nil,           
	)
 
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	
	msgs, err := channel.Consume(
		q.Name, 
		"",     
		true,   
		false,  
		false,  
		false,  
		nil,    
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message from RabbitMQ: %s", d.Body)

			
			var lead leadpb.LeadRequest
			err := json.Unmarshal(d.Body, &lead)
			if err != nil {
				log.Printf("Failed to unmarshal lead data: %v", err)
				continue
			}

		
			log.Printf("Processing lead from RabbitMQ: %+v", lead)
		}
	}()

	log.Printf(" [*] Waiting for messages from RabbitMQ. To exit press CTRL+C")
	<-forever
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

func main() {

	log.Println("comecou")

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

log.Println("Starting gRPC server... esse aqui")
	go startGrpcServer(channel)

	log.Println("Starting to consume leads from RabbitMQ...")
	go consumeLeadsFromRabbitMQ(channel)
	
	log.Println("Connecting to the database...")
	db.Connect()
	defer db.Close()

	
	db.Migrate()

	
	http.HandleFunc("/leads", leadHandler)

	
	port := os.Getenv("PORT")

	fmt.Println("API rodando na porta", port)
	if port == "" {
		log.Fatal("PORT não definido no ambiente")
	} else {
		fmt.Println("API rodando na porta", port)
	}
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
