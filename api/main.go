package main

import (
	"api/db"

	"context"
	"errors"
	"strings"

	"encoding/json"
	"fmt"
	"log"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"net/http"
	"os"

	"github.com/go-redis/redis/v8"
	"github.com/streadway/amqp"

	"time"
)

var redisClient *redis.Client
var ctx = context.Background()

func ConnectToRedis() {
	redisClient = redis.NewClient(&redis.Options{
		Addr: "redis:6379", 
		DB:   0,                
	})

	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Erro ao conectar ao Redis: %v", err)
	}
	log.Println("Conectado ao Redis com sucesso.")
}

func SaveLeadToRedis(googleId string, leadId uuid.UUID) error {
    if redisClient == nil {
        return fmt.Errorf("redisClient não inicializado")
    }

    if googleId == "" {
        return fmt.Errorf("googleId é vazio")
    }
    if leadId == uuid.Nil {
        return fmt.Errorf("leadId é inválido")
    }

    redisKey := fmt.Sprintf("google_lead:%s", googleId)

    err := redisClient.Set(ctx, redisKey, leadId.String(), 0).Err()
	if err != nil {
		return fmt.Errorf("Erro ao salvar google_id no Redis: %v", err)
	}

	log.Printf("Google ID %s salvo com Lead ID %s no Redis", googleId, leadId)
	return nil
}

func sendConfirmationToScrapper(googleId string) error {
    log.Printf("Iniciando envio de confirmação para o scrapper com Google ID: %s", googleId)

    // Reutiliza a conexão e o canal
    _, ch, err := connectToRabbitMQ()
    if err != nil {
        return fmt.Errorf("Erro ao conectar ao RabbitMQ: %v", err)
    }

    // Mensagem de confirmação
    message := map[string]string{
        "googleId": googleId,
        "status":   "ok",
    }
    body, err := json.Marshal(message)
    if err != nil {
        return fmt.Errorf("Falha ao codificar mensagem para o RabbitMQ: %v", err)
    }

    // Publica a mensagem
    err = ch.Publish(
        "scrapper_exchange", // exchange
        "",                  // routing key
        false,               // mandatory
        false,               // immediate
        amqp.Publishing{
            ContentType: "application/json",
            Body:        body,
        },
    )
    if err != nil {
        return fmt.Errorf("Falha ao publicar mensagem no RabbitMQ: %v", err)
    }

    log.Printf("Confirmação enviada para o scrapper: %v", message)
    return nil
}

func consumeCompaniesFromRabbitMQ(ch *amqp.Channel) {
	exchangeName := "companies_exchange"
	queueName := "companies_queue"

	err := ch.ExchangeDeclare(
		exchangeName, 
		"fanout",     
		true,         
		false,        
		false,        
		false,        
		nil,          
	)
	if err != nil {
		log.Fatalf("Failed to declare exchange: %v", err)
	}

	q, err := ch.QueueDeclare(
		queueName,
		true,  // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	err = ch.QueueBind(
		q.Name,        
		"",            
		exchangeName,  
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to bind queue to exchange: %v", err)
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

            log.Printf("Mensagem recebida em formato bruto consumeCompaniesFromRabbitMQ: %v", d.Body)

            log.Printf("Mensagem recebida do RabbitMQ pelo consumeCompaniesFromRabbitMQ: %s", string(d.Body))


			

			var combinedData map[string]interface{}

			err := json.Unmarshal(d.Body, &combinedData)
			if err != nil {
				log.Printf("Erro ao decodificar JSON: %v", err)
				continue
			}

			companiesInfo, ok := combinedData["companies_info"].([]interface{})
			if !ok {
				log.Printf("companies_info não encontrado na mensagem: %v", combinedData)
				continue
			}

			

			// Itera sobre a lista de companies_info e salva cada uma no banco de dados
			for _, companyInfo := range companiesInfo {
				companyMap, ok := companyInfo.(map[string]interface{})
				if !ok {
					log.Printf("Erro ao converter companyInfo para map: %v", companyInfo)
					continue
				}
                print("companyMap aqui",companyMap)

				// Salvar no banco de dados
				err = saveCompanyData(companyMap)
				if err != nil {
					log.Printf("Falha ao salvar empresa: %v", err)
				}
                cnpjDetails, ok := combinedData["cnpj_details"].([]interface{})
        if !ok {
            log.Printf("cnpj_details não encontrado ou não é uma lista válida: %v", combinedData)
            continue
        }

        // Processar cnpj_details se necessário
        for _, cnpjDetail := range cnpjDetails {
            cnpjMap, ok := cnpjDetail.(map[string]interface{})
            if !ok {
                log.Printf("Erro ao converter cnpjDetail para map: %v", cnpjDetail)
                continue
            }
            cnpjDetailJSON, err := json.MarshalIndent(cnpjMap, "", "  ")
            if err != nil {
            log.Printf("Erro ao serializar detalhes do CNPJ para JSON: %v", err)
            continue
            }

            // Aqui você pode decidir o que fazer com os detalhes do CNPJ
            log.Printf("Detalhes do CNPJ:\n%s", string(cnpjDetailJSON))
        }
			}
		}
	}()

	log.Println("Consumindo empresas do RabbitMQ...")
	select {} // Block para manter o consumer ativo
}

func saveCompanyData(data map[string]interface{}) error {
    lead := db.Lead{}
    lead_step := db.LeadStep{}

    if v, ok := data["google_id"].(string); ok {
        existingLead, err := db.GetLeadByGoogleId(v)
        if err != nil {
            if errors.Is(err, gorm.ErrRecordNotFound) {
               
                lead.GoogleId = v
                lead.CompanyRegistrationID = data["company_cnpj"].(string)
                lead.RegisteredName = data["company_name"].(string)
                lead.City = data["company_city"].(string)

                err = db.CreateLead(&lead)
                if err != nil {
                    return fmt.Errorf("Erro ao criar lead: %v", err)
                }

               
                lead_step.LeadID = lead.ID
                lead_step.Step = "Empresa Criada"
                lead_step.Status = "Sucesso"
                lead_step.Details = fmt.Sprintf("Empresa %s com CNPJ %s criada", lead.RegisteredName, lead.CompanyRegistrationID)

                err = db.CreateLeadStep(&lead_step)
                if err != nil {
                    return fmt.Errorf("Erro ao criar LeadStep: %v", err)
                }
            } else {
               
                return fmt.Errorf("Erro ao buscar lead: %v", err)
            }
        } else {
           
            existingLead.CompanyRegistrationID = data["company_cnpj"].(string)
            existingLead.RegisteredName = data["company_name"].(string)
            existingLead.City = data["company_city"].(string)

			log.Printf("Iniciando atualização do Lead com Google ID: %s", existingLead.GoogleId)
            err = db.UpdateLead(existingLead)
			log.Printf("Atualização do Lead com Google ID: %s concluída", existingLead.GoogleId)
            if err != nil {
				log.Printf("Erro ao atualizar o lead: %v", err)
                return fmt.Errorf("Erro ao atualizar o lead: %v", err)
            }

         
            lead_step.LeadID = existingLead.ID
            lead_step.Step = "Empresa Atualizada"
            lead_step.Status = "Sucesso"
            lead_step.Details = fmt.Sprintf("Empresa %s com CNPJ %s atualizada", existingLead.RegisteredName, existingLead.CompanyRegistrationID)

            err = db.CreateLeadStep(&lead_step)
            if err != nil {
                return fmt.Errorf("Erro ao criar LeadStep: %v", err)
            }
        }
    }
    return nil
}

func leadHandler(w http.ResponseWriter, r *http.Request) {
    var lead db.Lead
    var lead_step db.LeadStep

    // Decode o corpo da requisição
    err := json.NewDecoder(r.Body).Decode(&lead)
    if err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    // Tente buscar o Lead pelo Google ID
    existingLead, err := db.GetLeadByGoogleId(lead.GoogleId)
    if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
        log.Printf("Erro ao buscar lead com Google ID: %s - %v", lead.GoogleId, err)
        http.Error(w, "Erro ao buscar lead", http.StatusInternalServerError)
        return
    }

    // Se o lead existir, atualize
        log.Printf("Chamando update1 Lead para Google ID: %s", existingLead.GoogleId)
        existingLead.CompanyRegistrationID = lead.CompanyRegistrationID
        existingLead.RegisteredName = lead.RegisteredName
        log.Printf("Chamando update1 existingLead.RegisteredName: %s", existingLead.RegisteredName)
        log.Printf("Chamando sendConfirmationToScrapper para Google antes ID: %s", existingLead.GoogleId)
        err = sendConfirmationToScrapper(existingLead.GoogleId)
        if err != nil {
            log.Printf("Erro ao enviar confirmação para o scrapper antes: %v", err)
            http.Error(w, "Falha ao enviar confirmação para o scrapper antes", http.StatusInternalServerError)
            return
        }
        err = db.UpdateLead(existingLead)
        if err != nil {
            log.Printf("Erro ao atualizar o lead: %v", err)
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return

        }
        log.Printf("Lead atualizado com sucesso na tabela1 para Google ID: %s", existingLead.GoogleId)

        // Crie o LeadStep para a atualização
		log.Printf("Chamando update Lead para Google ID: %s", existingLead.GoogleId)
        lead_step.LeadID = existingLead.ID
        lead_step.Step = "Lead Atualizado"
        lead_step.Status = "Sucesso"
        lead_step.Details = fmt.Sprintf("Lead %s foi atualizado com CNPJ %s", existingLead.BusinessName, existingLead.CompanyRegistrationID)
        err = db.CreateLeadStep(&lead_step)
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }

		log.Printf("Chamando sendConfirmationToScrapper para Google ID: %s", existingLead.GoogleId)
        err = sendConfirmationToScrapper(existingLead.GoogleId)
        if err != nil {
            log.Printf("Erro ao enviar confirmação para o scrapper: %v", err)
            http.Error(w, "Falha ao enviar confirmação para o scrapper", http.StatusInternalServerError)
            return
        }

   
    w.WriteHeader(http.StatusCreated)
    fmt.Fprintf(w, "Lead e LeadStep salvos com sucesso!")
}

var rabbitConn *amqp.Connection
var rabbitChannel *amqp.Channel


func connectToRabbitMQ() (*amqp.Connection, *amqp.Channel, error) {

    if rabbitConn != nil && rabbitChannel != nil {
        return rabbitConn, rabbitChannel, nil
    }

    rabbitmqHost := os.Getenv("RABBITMQ_HOST")
    rabbitmqPort := os.Getenv("RABBITMQ_PORT")

     if rabbitmqHost == "" || rabbitmqPort == "" {
        return nil, nil, fmt.Errorf("RABBITMQ_HOST and RABBITMQ_PORT must be set")
    }

	var conn *amqp.Connection
    var ch *amqp.Channel
    var err error

   for i := 0; i < 5; i++ {
        conn, err = amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%s/", rabbitmqHost, rabbitmqPort))
        if err == nil {
            break
        }
        log.Printf("Failed to connect to RabbitMQ at %s:%s, retrying in 10 seconds... (%d/5)", rabbitmqHost, rabbitmqPort, i+1)
        time.Sleep(10 * time.Second)
    }

    if err != nil {
        return nil, nil, fmt.Errorf("failed to connect to RabbitMQ after 5 retries: %v", err)
    }

    // Abre o canal do RabbitMQ
    ch, err = conn.Channel()
    if err != nil {
        return nil, nil, fmt.Errorf("failed to open RabbitMQ channel: %v", err)
    }

    rabbitConn = conn
    rabbitChannel = ch

    return conn, ch, nil

}

func consumeLeadsFromRabbitMQ(ch *amqp.Channel) {
	exchangeName := "leads_exchange"
    queueName := "leads_queue"

	err := ch.ExchangeDeclare(
        exchangeName, 
        "fanout",     
        true,         
        false,        
        false,        
        false,        
        nil,          
    )
    if err != nil {
        log.Fatalf("Failed to declare exchange: %v", err)
    }

  q, err := ch.QueueDeclare(
        queueName,
        true, // Durable
        false, // Delete when unused
        false, // Exclusive
        false, // No-wait
        nil,   // Arguments
    )
    if err != nil {
        log.Fatalf("Failed to register a consumer: %v", err)
    }

	err = ch.QueueBind(
        q.Name,        
        "",            
        exchangeName,  
        false,
        nil,
    )
    if err != nil {
        log.Fatalf("Failed to bind queue to exchange: %v", err)
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
            log.Printf("Mensagem recebida do RabbitMQ pelo consumeLeadsFromRabbitMQ: %s", string(d.Body))
            var leadData map[string]interface{}
            err := json.Unmarshal(d.Body, &leadData)
            if err != nil {
                log.Printf("Error decoding JSON: %v", err)
                continue
            }

       
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
		if lead.Address == "" {
			log.Printf("Aviso: Endereço vazio para o PlaceID %s", data["PlaceID"])
		}
	}

	if v, ok := data["City"].(string); ok {
        lead.City = v
    }
    if v, ok := data["State"].(string); ok {
        lead.State = v
    }
    if v, ok := data["ZIPCode"].(string); ok {
        lead.ZIPCode = v
    }

	if v, ok := data["Country"].(string); ok {
        lead.Country = v
    }

    if v, ok := data["InternationalPhoneNumber"].(string); ok {
        lead.Phone = v
    }
    if v, ok := data["Website"].(string); ok {
        lead.Website = v
        
        if strings.HasPrefix(lead.Website, "https://www.instagram.com") {
            lead.Instagram = lead.Website
            lead.Website = ""
        }
    }

	if v, ok := data["Description"].(string); ok {
        if lead.Description != "" {
           
            lead.Description = fmt.Sprintf("%s\n%s", lead.Description, v)
        } else {
            lead.Description = v
        }
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
		var types []string
		for _, t := range v {
			if typeStr, ok := t.(string); ok {
				types = append(types, typeStr)
			}
		}
		lead.Categories = strings.Join(types, ", ")
	}
	
	if category, ok := data["Category"].(string); ok {
        if city, ok := data["City"].(string); ok {
            radius := data["Radius"]
            lead.SearchTerm = fmt.Sprintf("%s, %s, %v", category, city, radius)
        }
    }
	if v, ok := data["PlaceID"].(string); ok {
        lead.GoogleId = v
    }

	lead.Source = "GooglePlaces"

 
    err := db.CreateLead(&lead)
    if err != nil {
        return fmt.Errorf("Failed to save lead to database: %v", err)
    }



    log.Printf("Lead saved to database: %v", lead)

    err = SaveLeadToRedis(lead.GoogleId, lead.ID)
    if err != nil {
        return fmt.Errorf("Failed to save lead to Redis: %v", err)
    }

    log.Printf("Lead salvo no Redis: Google ID %s -> Lead ID %s", lead.GoogleId, lead.ID)
    return nil
}

func closeRabbitMQ() {
    if rabbitChannel != nil {
        rabbitChannel.Close()
    }
    if rabbitConn != nil {
        rabbitConn.Close()
    }
}

func setupChannel(connection *amqp.Connection) (*amqp.Channel, error) {
    channel, err := connection.Channel()
    if err != nil {
        return nil, fmt.Errorf("Erro ao abrir canal: %v", err)
    }

    // Declaração da exchange scrapper_exchange
    exchangeName := "scrapper_exchange"
    err = channel.ExchangeDeclare(
        exchangeName,
        "fanout",     // Tipo de exchange
        true,         // Durável
        false,        // Auto-delete
        false,        // Interna
        false,        // No-wait
        nil,          // Argumentos adicionais
    )
    if err != nil {
        return nil, fmt.Errorf("Erro ao declarar exchange: %v", err)
    }

    log.Printf("Exchange %s declarada com sucesso", exchangeName)
    return channel, nil
}



func main() {

	log.Println("Starting API service...")

    _, channel, err := connectToRabbitMQ() 
	if err != nil {
		log.Fatalf("Erro ao conectar ao RabbitMQ: %v", err)
	}
	defer closeRabbitMQ()

    log.Println("Conectando ao Redis...")
    ConnectToRedis()

    if redisClient == nil {
        log.Fatalf("Falha ao conectar ao Redis.")
    }

	log.Println("Connecting to the database...")
	db.Connect()

	defer db.Close()
	db.Migrate()

    log.Println("Starting to consume leads from RabbitMQ...")

 	
    go consumeLeadsFromRabbitMQ(channel)

   
    go consumeCompaniesFromRabbitMQ(channel)
	
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
