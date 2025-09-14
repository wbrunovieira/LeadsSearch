package main

import (
	"api/db"
	"bytes"
	"database/sql"

	"regexp"
	"strconv"

	"context"
	"errors"
	"strings"

	"encoding/json"
	"fmt"
	"log"

	"github.com/joho/godotenv"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"io"
	"net/http"
	"os"

	"github.com/go-redis/redis/v8"
	"github.com/streadway/amqp"

	"time"
)

var redisClient *redis.Client
var ctx = context.Background()

var rabbitConn *amqp.Connection
var rabbitChannel *amqp.Channel

var temporaryErrors = []error{
	sql.ErrConnDone,
	sql.ErrTxDone,
}

func main() {
	log.Println("Starting API service...")

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Erro ao carregar .env:", err)
	}

	conn, err := connectToRabbitMQ()
	if err != nil {
		log.Fatalf("Erro ao conectar ao RabbitMQ: %v", err)
	}
	defer conn.Close()

	leadsChannel, err := setupChannel(conn)
	if err != nil {
		log.Fatalf("Erro ao configurar canal para leads: %v", err)
	}
	companiesChannel, err := setupChannel(conn)
	if err != nil {
		log.Fatalf("Erro ao configurar canal para companies: %v", err)
	}
	googlePlacesChannel, err := setupChannel(conn)
	if err != nil {
		log.Fatalf("Erro ao configurar canal para Google Places: %v", err)
	}
	defer leadsChannel.Close()
	defer companiesChannel.Close()
	defer googlePlacesChannel.Close()

	log.Println("Conectando ao Redis...")
	if err := ConnectToRedis(); err != nil {
		log.Fatalf("Erro ao conectar ao Redis: %v", err)
	}
	if redisClient == nil {
		log.Fatalf("Falha ao conectar ao Redis.")
	}

	log.Println("Conectando ao banco de dados...")
	if err := db.Connect(); err != nil {
		log.Fatalf("Erro ao conectar ao banco de dados: %v", err)
	}
	defer db.Close()

	if err := db.Migrate(); err != nil {
		log.Fatalf("Erro ao migrar o banco de dados: %v", err)
	}

	log.Println("Starting to consume leads from RabbitMQ...")
	go consumeLeadsFromRabbitMQ(leadsChannel)

	log.Println("Starting to consume companies from RabbitMQ...")
	go consumeCompaniesFromRabbitMQ(companiesChannel)

	log.Println("Starting to consume Google Places leads from RabbitMQ...")
	go consumeGooglePlacesLeads(googlePlacesChannel)

	http.HandleFunc("/leads", leadHandler)

	port := os.Getenv("PORT")
	if port == "" {
		log.Fatal("PORT não definido no ambiente")
	}
	fmt.Println("API rodando na porta", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func consumeGooglePlacesLeads(ch *amqp.Channel) {
	queueName := "leads_exchange"

	err := ch.ExchangeDeclare(
		queueName, // Exchange de onde as mensagens vêm
		"fanout",  // Tipo de exchange
		true,      // Durável
		false,     // Auto-delete
		false,     // Interno
		false,     // No-wait
		nil,       // Argumentos adicionais
	)
	if err != nil {
		log.Fatalf("Erro ao declarar exchange: %v", err)
	}

	q, err := ch.QueueDeclare(
		"",
		false, // Não persistente
		false, // Auto-delete
		true,  // Exclusivo para esta conexão
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Erro ao declarar fila: %v", err)
	}

	err = ch.QueueBind(
		q.Name,
		"",
		queueName,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Erro ao associar fila ao exchange: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		false, // Manual ACK
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Erro ao registrar consumidor: %v", err)
	}

	log.Println("Consumindo leads do Google Places...")

	go func() {
		for d := range msgs {
			log.Printf("Lead recebido do Google Places: %s", string(d.Body))

			var leadData map[string]interface{}
			if err := json.Unmarshal(d.Body, &leadData); err != nil {
				log.Printf("Erro ao decodificar JSON: %v", err)
				d.Nack(false, true) // Reenviar a mensagem para a fila
				continue
			}

			err := saveLeadToDatabase(leadData)
			if err != nil {
				log.Printf("Erro ao salvar lead no banco de dados: %v", err)
				d.Nack(false, false) // Descartar a mensagem
				continue
			}

			d.Ack(false) // Confirmação de processamento bem-sucedido
			log.Println("Lead do Google Places salvo com sucesso!")
		}
	}()
}

func hasWhatsApp(phone string) (bool, error) {

	apiKey := os.Getenv("WHATSAPP_API_KEY")
	if apiKey == "" {
		log.Fatal("Erro: API Key não encontrada nas variáveis de ambiente")
	} else {
		log.Println("API Key carregada com sucesso!")
	}

	url := fmt.Sprintf("https://whatsapp-api.wbdigitalsolutions.com/chat/whatsappNumbers/%s", os.Getenv("WHATSAPP_API_USER"))
	payload, _ := json.Marshal(map[string][]string{"numbers": {phone}})

	request, err := http.NewRequest("POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return false, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("apikey", apiKey)

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return false, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return false, err
	}

	if response.StatusCode != http.StatusOK {
		return false, fmt.Errorf("Erro na requisição: %s", response.Status)
	}

	var result []map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return false, err
	}

	if len(result) > 0 {
		if exists, ok := result[0]["exists"].(bool); ok {
			return exists, nil
		}
	}

	return false, fmt.Errorf("Resposta inesperada da API")
}

func validateEmail(email string) (bool, error) {
	url := "http://validator-service:8090/validate_email"
	client := &http.Client{}

	payload := fmt.Sprintf(`["%s"]`, email)
	request, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(payload)))
	if err != nil {
		return false, err
	}
	headers := map[string]string{
		"Content-Type": "application/json",
	}
	for k, v := range headers {
		request.Header.Set(k, v)
	}

	response, err := client.Do(request)
	if err != nil {
		return false, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return false, fmt.Errorf("Erro ao validar o email: %s", response.Status)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return false, err
	}

	var results []map[string]interface{}
	if err := json.Unmarshal(body, &results); err != nil {
		return false, err
	}

	if len(results) > 0 {
		return results[0]["is_valid"].(bool), nil
	}

	return false, fmt.Errorf("Resultado inválido para validação de email")
}

func connectToRabbitMQ() (*amqp.Connection, error) {
	rabbitmqHost := os.Getenv("RABBITMQ_HOST")
	rabbitmqPort := os.Getenv("RABBITMQ_PORT")

	if rabbitmqHost == "" || rabbitmqPort == "" {
		return nil, fmt.Errorf("RABBITMQ_HOST and RABBITMQ_PORT must be set")
	}

	var conn *amqp.Connection
	var err error

	log.Printf("Tentando conectar ao RabbitMQ: %s:%s", rabbitmqHost, rabbitmqPort)

	for i := 0; i < 5; i++ {
		conn, err = amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%s/", rabbitmqHost, rabbitmqPort))
		if err == nil {
			log.Println("Conexão com RabbitMQ bem-sucedida")
			break
		}
		log.Printf("Falha ao conectar ao RabbitMQ, tentando novamente em 10 segundos... (%d/5)", i+1)
		time.Sleep(10 * time.Second)
	}

	if err != nil {
		return nil, fmt.Errorf("Falha ao conectar ao RabbitMQ após 5 tentativas: %v", err)
	}

	rabbitConn = conn
	return conn, nil
}

func ConnectToRedis() error {
	redisClient = redis.NewClient(&redis.Options{
		Addr: "redis:6379",
		DB:   0,
	})

	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Erro ao conectar ao Redis: %v", err)
	}
	log.Println("Conectado ao Redis com sucesso.")
	return nil
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
	log.Printf("Iniciando envio de dados para o scrapper com Google ID: %s", googleId)

	// Buscar o lead do banco de dados usando o google_id
	lead, err := db.GetLeadByGoogleId(googleId)
	if err != nil {
		return fmt.Errorf("Erro ao buscar lead com Google ID %s: %v", googleId, err)
	}

	conn, err := connectToRabbitMQ()
	if err != nil {
		return fmt.Errorf("Erro ao conectar ao RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := setupChannel(conn)
	if err != nil {
		return fmt.Errorf("Erro ao configurar o canal: %v", err)
	}
	defer ch.Close()

	// Enviar dados completos para o scrapper
	message := map[string]interface{}{
		"GoogleId": googleId,
		"Name":     lead.BusinessName,
		"City":     lead.City,
		"State":    lead.State,
		"Address":  lead.Address,
		"Phone":    lead.Phone,
		"Website":  lead.Website,
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

	log.Printf("Dados enviados para o scrapper: %v", message)
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
		true,
		false,
		false,
		false,
		nil,
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

	err = ch.Qos(5, 0, false)
	if err != nil {
		log.Fatalf("Failed to set QoS: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		false, // Manual ack
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	go func() {
		for d := range msgs {

			log.Printf("Mensagem recebida do scrapper via companies_exchange: %s", string(d.Body))

			var combinedData map[string]interface{}

			err := json.Unmarshal(d.Body, &combinedData)
			if err != nil {
				log.Printf("Erro ao decodificar JSON: %v", err)
				d.Nack(false, true)
				continue
			}

			// Buscar o google_id dos dados
			googleId, ok := combinedData["google_id"].(string)
			if !ok {
				log.Printf("google_id não encontrado na mensagem: %v", combinedData)
				d.Nack(false, true)
				continue
			}

			// Buscar o lead_id usando o google_id do Redis
			leadIdStr, err := redisClient.Get(ctx, fmt.Sprintf("google_lead:%s", googleId)).Result()
			if err != nil {
				log.Printf("Erro ao buscar lead_id no Redis para google_id %s: %v", googleId, err)
				d.Nack(false, true)
				continue
			}

			leadId, err := uuid.Parse(leadIdStr)
			if err != nil {
				log.Printf("Erro ao fazer parse do lead_id %s: %v", leadIdStr, err)
				d.Nack(false, false)
				continue
			}

			// Processar dados do CNPJ
			cnpjDataList, ok := combinedData["cnpj_data"].([]interface{})
			if !ok || len(cnpjDataList) == 0 {
				log.Printf("cnpj_data não encontrado ou vazio: %v", combinedData)
				d.Nack(false, false)
				continue
			}

			// Usar o primeiro CNPJ válido encontrado
			for _, cnpjData := range cnpjDataList {
				cnpjMap, ok := cnpjData.(map[string]interface{})
				if !ok {
					continue
				}

				// Atualizar o lead com os dados do CNPJ
				err = updateLeadWithCNPJData(leadId, cnpjMap)
				if err != nil {
					log.Printf("Erro ao atualizar lead %s com dados do CNPJ: %v", leadId, err)
					continue
				}

				log.Printf("Lead %s atualizado com sucesso com dados do CNPJ", leadId)
				break // Usar apenas o primeiro CNPJ válido
			}

			d.Ack(false) // Confirmar processamento
			log.Printf("Dados do CNPJ processados com sucesso para google_id: %s, lead_id: %s", googleId, leadId)
		}
	}()

	log.Println("Consumindo empresas do RabbitMQ...")
	select {} // Block para manter o consumer ativo
}

func saveLeadToDatabase(data map[string]interface{}) error {
	log.Println("Iniciando processamento do lead...")
	lead := db.Lead{}

	if lead.ID == uuid.Nil {
		lead.ID = uuid.New()
		log.Printf("Gerado novo UUID para lead: %s", lead.ID)
	}

	if v, ok := data["Name"].(string); ok {
		lead.BusinessName = v
		log.Printf("Nome do negócio: %s", lead.BusinessName)
	}

	if v, ok := data["FormattedAddress"].(string); ok {
		lead.Address = v
		log.Printf("Endereço formatado: %s", lead.Address)
		if lead.Address == "" {
			log.Printf("Aviso: Endereço vazio para o PlaceID %s", data["PlaceID"])
		}
	}

	if v, ok := data["City"].(string); ok {
		lead.City = v
		log.Printf("Cidade: %s", lead.City)
	}
	if v, ok := data["State"].(string); ok {
		lead.State = v
		log.Printf("Estado: %s", lead.State)
	}
	if v, ok := data["ZIPCode"].(string); ok {
		lead.ZIPCode = v
		log.Printf("CEP: %s", lead.ZIPCode)
	}

	if v, ok := data["Country"].(string); ok {
		lead.Country = v
		log.Printf("País: %s", lead.Country)
	}

	if v, ok := data["InternationalPhoneNumber"].(string); ok {
		log.Printf("Verificando WhatsApp para o telefone: %s", v)
		hasWhatsapp, err := hasWhatsApp(v)
		if err != nil {
			log.Printf("Erro ao verificar WhatsApp: %v", err)
		} else if hasWhatsapp {
			lead.Whatsapp = v
			log.Println("WhatsApp confirmado e salvo.", lead.Whatsapp)
		}
		lead.Phone = v
	}
	log.Printf("Lead preparado para salvar - Nome: %s, Phone: %s, WhatsApp: %s", lead.BusinessName, lead.Phone, lead.Whatsapp)

	if v, ok := data["Email"].(string); ok {
		log.Printf("Validando email: %s", v)
		isValidEmail, err := validateEmail(v)
		if err != nil {
			log.Printf("Erro ao validar email: %v", err)
		} else if isValidEmail {
			lead.Email = v
			log.Println("Email válido salvo.")
		} else {
			log.Printf("Email inválido, não será salvo: %s", v)
		}
	}

	if v, ok := data["Website"].(string); ok {
		lead.Website = v
		log.Printf("Website: %s", lead.Website)
		if strings.HasPrefix(lead.Website, "https://www.instagram.com") {
			lead.Instagram = lead.Website
			lead.Website = ""
			log.Println("Detectado Instagram, atualizado corretamente.")
		}
		if strings.HasPrefix(lead.Website, "https://www.facebook.com.com") {
			lead.Facebook = lead.Website
			lead.Website = ""
			log.Println("Detectado Facebook, atualizado corretamente.")
		}
	}

	if v, ok := data["Description"].(string); ok {
		if lead.Description != "" {
			lead.Description = fmt.Sprintf("%s\n%s", lead.Description, v)
		} else {
			lead.Description = v
		}
		log.Println("Descrição atualizada.")
	}

	if v, ok := data["Rating"].(float64); ok {
		lead.Rating = v
		log.Printf("Avaliação: %.2f", lead.Rating)
	}
	if v, ok := data["UserRatingsTotal"].(float64); ok {
		lead.UserRatingsTotal = int(v)
		log.Printf("Total de avaliações: %d", lead.UserRatingsTotal)
	}
	if v, ok := data["PriceLevel"].(float64); ok {
		lead.PriceLevel = int(v)
		log.Printf("Nível de preço: %d", lead.PriceLevel)
	}
	if v, ok := data["BusinessStatus"].(string); ok {
		lead.BusinessStatus = v
		log.Printf("Status do negócio: %s", lead.BusinessStatus)
	}
	if v, ok := data["Vicinity"].(string); ok {
		lead.Vicinity = v
	}
	if v, ok := data["PermanentlyClosed"].(bool); ok {
		lead.PermanentlyClosed = v
		log.Printf("Fechado permanentemente: %v", lead.PermanentlyClosed)
	}
	if v, ok := data["Types"].([]interface{}); ok {
		var types []string
		for _, t := range v {
			if typeStr, ok := t.(string); ok {
				types = append(types, typeStr)
			}
		}
		lead.Categories = strings.Join(types, ", ")
		log.Printf("Categorias: %s", lead.Categories)
	}

	if category, ok := data["Category"].(string); ok {
		if city, ok := data["City"].(string); ok {
			radius := data["Radius"]
			lead.SearchTerm = fmt.Sprintf("%s, %s, %v", category, city, radius)
			log.Printf("Termo de busca salvo: %s", lead.SearchTerm)
		}
	}

	if v, ok := data["PlaceID"].(string); ok {
		lead.GoogleId = v
		log.Printf("Google ID: %s", lead.GoogleId)
	}

	lead.Source = "GooglePlaces"
	log.Println("Tentando salvar lead no banco de dados...")
	err := db.CreateLead(&lead)
	if err != nil {
		log.Printf("Erro ao salvar lead no banco de dados: %v", err)
		return fmt.Errorf("Failed to save lead to database: %v", err)
	}
	log.Printf("Lead salvo no banco de dados: %v", lead)

	log.Println("Tentando salvar lead no Redis...")
	err = SaveLeadToRedis(lead.GoogleId, lead.ID)
	if err != nil {
		log.Printf("Erro ao salvar lead no Redis: %v", err)
		return fmt.Errorf("Failed to save lead to Redis: %v", err)
	}

	log.Printf("Lead salvo no Redis: Google ID %s -> Lead ID %s", lead.GoogleId, lead.ID)

	// Enviar confirmação para o scrapper processar o lead
	log.Printf("Enviando lead para o scrapper: Google ID %s", lead.GoogleId)
	err = sendConfirmationToScrapper(lead.GoogleId)
	if err != nil {
		log.Printf("Erro ao enviar lead para o scrapper: %v", err)
		// Não retornamos erro aqui para não falhar o salvamento do lead
		// O scrapper pode processar posteriormente via retry ou outra lógica
	} else {
		log.Printf("Lead enviado com sucesso para o scrapper")
	}

	return nil
}

func saveCompanyData(data map[string]interface{}) (uuid.UUID, error) {
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
					return uuid.Nil, fmt.Errorf("Erro ao criar lead: %v", err)
				}

				lead_step.LeadID = lead.ID
				lead_step.Step = "Empresa Criada"
				lead_step.Status = "Sucesso"
				lead_step.Details = fmt.Sprintf("Empresa %s com CNPJ %s criada", lead.RegisteredName, lead.CompanyRegistrationID)

				err = db.CreateLeadStep(&lead_step)
				if err != nil {
					return uuid.Nil, fmt.Errorf("Erro ao criar LeadStep: %v", err)
				}
			} else {

				return uuid.Nil, fmt.Errorf("Erro ao buscar lead: %v", err)
			}
		} else {

			existingLead.CompanyRegistrationID = data["company_cnpj"].(string)
			existingLead.RegisteredName = data["company_name"].(string)
			existingLead.City = data["company_city"].(string)

			log.Printf("Iniciando atualização do Lead com Google ID: %s", existingLead.GoogleId)
			err = db.UpdateLead(existingLead)
			log.Printf("Atualização do Lead com Google ID: %s concluída", existingLead.GoogleId)
			if err != nil {
				return uuid.Nil, fmt.Errorf("Erro ao atualizar o lead: %v", err)
			}

			lead_step.LeadID = existingLead.ID
			lead_step.Step = "Empresa Atualizada"
			lead_step.Status = "Sucesso"
			lead_step.Details = fmt.Sprintf("Empresa %s com CNPJ %s atualizada", existingLead.RegisteredName, existingLead.CompanyRegistrationID)

			err = db.CreateLeadStep(&lead_step)
			if err != nil {
				return uuid.Nil, fmt.Errorf("Erro ao criar LeadStep: %v", err)
			}
			return existingLead.ID, nil
		}
	}
	return lead.ID, nil
}

func updateLeadWithCNPJData(leadID uuid.UUID, cnpjData map[string]interface{}) error {
	lead, err := db.GetLeadByID(leadID)
	if err != nil {
		return fmt.Errorf("Erro ao buscar Lead com ID %s: %v", leadID, err)
	}

	if lead == nil {
		return fmt.Errorf("Lead não encontrado com ID: %s", leadID)
	}

	// Atualizar CNPJ
	if cnpj, ok := cnpjData["cnpj"].(string); ok && cnpj != "" {
		// Formatar CNPJ se necessário
		if len(cnpj) == 14 {
			cnpj = fmt.Sprintf("%s.%s.%s/%s-%s", cnpj[:2], cnpj[2:5], cnpj[5:8], cnpj[8:12], cnpj[12:])
		}
		lead.CompanyRegistrationID = cnpj
		log.Printf("CNPJ atualizado: %s", cnpj)
	}

	// Atualizar Razão Social
	if razaoSocial, ok := cnpjData["razao_social"].(string); ok && razaoSocial != "" {
		lead.RegisteredName = razaoSocial
		log.Printf("Razão Social atualizada: %s", razaoSocial)
	}

	// Atualizar Nome Fantasia
	if nomeFantasia, ok := cnpjData["nome_fantasia"].(string); ok && nomeFantasia != "" {
		if lead.BusinessName == "" || len(nomeFantasia) > len(lead.BusinessName) {
			lead.BusinessName = nomeFantasia
		}
	}

	// Atualizar Email
	if email, ok := cnpjData["email"].(string); ok && email != "" {
		lead.Email = email
		log.Printf("Email atualizado: %s", email)
	}

	// Atualizar Telefones
	if telefone1, ok := cnpjData["telefone1"].(string); ok && telefone1 != "" {
		if lead.Phone == "" {
			lead.Phone = telefone1
		}
	}

	// Atualizar Capital Social
	if capitalStr, ok := cnpjData["capital_social"].(string); ok && capitalStr != "" {
		if capital, err := strconv.ParseFloat(capitalStr, 64); err == nil {
			lead.EquityCapital = capital
			log.Printf("Capital Social atualizado: %.2f", capital)
		}
	}

	// Atualizar Data de Fundação
	if dataInicio, ok := cnpjData["data_inicio"].(string); ok && dataInicio != "" {
		// Converter formato DD/MM/YYYY para YYYY-MM-DD
		parts := strings.Split(dataInicio, "/")
		if len(parts) == 3 {
			formattedDate := fmt.Sprintf("%s-%s-%s", parts[2], parts[1], parts[0])
			if t, err := time.Parse("2006-01-02", formattedDate); err == nil {
				lead.FoundationDate = sql.NullTime{Time: t, Valid: true}
				log.Printf("Data de fundação atualizada: %s", formattedDate)
			}
		}
	}

	// Atualizar Porte da Empresa
	if porte, ok := cnpjData["porte"].(string); ok && porte != "" {
		lead.CompanySize = porte
		log.Printf("Porte atualizado: %s", porte)
	}

	// Atualizar Situação
	if situacao, ok := cnpjData["situacao"].(map[string]interface{}); ok {
		if nome, ok := situacao["nome"].(string); ok && nome != "" {
			lead.BusinessStatus = nome
			log.Printf("Situação atualizada: %s", nome)
		}
	}

	// Atualizar Sócios (owners)
	if socios, ok := cnpjData["socios"].([]interface{}); ok && len(socios) > 0 {
		var owners []string
		for _, socio := range socios {
			if socioMap, ok := socio.(map[string]interface{}); ok {
				if nome, ok := socioMap["nome"].(string); ok && nome != "" {
					if qualificacao, ok := socioMap["qualificacao"].(string); ok && qualificacao != "" {
						owners = append(owners, fmt.Sprintf("%s (%s)", nome, qualificacao))
					} else {
						owners = append(owners, nome)
					}
				}
			}
		}
		if len(owners) > 0 {
			lead.Owner = strings.Join(owners, "; ")
			log.Printf("Sócios atualizados: %s", lead.Owner)
		}
	}

	// Atualizar Atividade Principal
	if atividadePrincipal, ok := cnpjData["atividade_principal"].(map[string]interface{}); ok {
		if descricao, ok := atividadePrincipal["text"].(string); ok && descricao != "" {
			lead.PrimaryActivity = descricao
			log.Printf("Atividade principal atualizada: %s", descricao)
		}
	}

	// Atualizar Atividades Secundárias
	if atividadesSecundarias, ok := cnpjData["atividades_secundarias"].([]interface{}); ok {
		var activities []string
		for _, atividade := range atividadesSecundarias {
			if atividadeMap, ok := atividade.(map[string]interface{}); ok {
				if descricao, ok := atividadeMap["text"].(string); ok && descricao != "" {
					activities = append(activities, descricao)
				}
			}
		}
		if len(activities) > 0 {
			lead.SecondaryActivities = strings.Join(activities, "; ")
			log.Printf("Atividades secundárias atualizadas: %d atividades", len(activities))
		}
	}

	// Atualizar Endereço se mais completo
	if endereco, ok := cnpjData["endereco"].(map[string]interface{}); ok {
		var addressParts []string
		if logradouro, ok := endereco["logradouro"].(string); ok && logradouro != "" {
			if numero, ok := endereco["numero"].(string); ok && numero != "" {
				addressParts = append(addressParts, fmt.Sprintf("%s, %s", logradouro, numero))
			} else {
				addressParts = append(addressParts, logradouro)
			}
		}
		if bairro, ok := endereco["bairro"].(string); ok && bairro != "" {
			addressParts = append(addressParts, bairro)
		}
		if len(addressParts) > 0 {
			newAddress := strings.Join(addressParts, " - ")
			if len(newAddress) > len(lead.Address) {
				lead.Address = newAddress
			}
		}

		// Atualizar CEP se disponível
		if cep, ok := endereco["cep"].(string); ok && cep != "" {
			lead.ZIPCode = cep
		}
	}

	// Atualizar descrição com informações adicionais
	var additionalInfo []string
	if natureza, ok := cnpjData["natureza_juridica"].(string); ok && natureza != "" {
		additionalInfo = append(additionalInfo, fmt.Sprintf("Natureza Jurídica: %s", natureza))
	}
	if tipo, ok := cnpjData["tipo"].(string); ok && tipo != "" {
		additionalInfo = append(additionalInfo, fmt.Sprintf("Tipo: %s", tipo))
	}

	if len(additionalInfo) > 0 {
		newInfo := strings.Join(additionalInfo, "\n")
		if lead.Description == "" {
			lead.Description = newInfo
		} else {
			lead.Description = fmt.Sprintf("%s\n%s", lead.Description, newInfo)
		}
	}

	// Salvar o lead atualizado
	err = db.UpdateLead(lead)
	if err != nil {
		return fmt.Errorf("Erro ao atualizar lead no banco de dados: %v", err)
	}

	log.Printf("Lead %s atualizado com sucesso com dados do CNPJ", leadID)
	return nil
}

func updateLeadWithCNPJDetailsByID(leadID uuid.UUID, cnpjDetails map[string]interface{}) error {
	lead, err := db.GetLeadByID(leadID)
	if err != nil {
		return fmt.Errorf("Erro ao buscar Lead com ID %s: %v", leadID, err)
	}

	if lead == nil {
		return fmt.Errorf("Lead não encontrado com ID: %s", leadID)
	}

	var newDescriptions []string

	if v, ok := cnpjDetails["razao_social"].(string); ok {
		razaoSocialUpdate := fmt.Sprintf("Razão Social encontrada na Receita Federal: %s", v)
		newDescriptions = append(newDescriptions, razaoSocialUpdate)
	}

	if v, ok := cnpjDetails["atividade_principal"].(map[string]interface{}); ok {
		if descricao, ok := v["descricao"].(string); ok {
			newDescriptions = append(newDescriptions, fmt.Sprintf("Atividade Principal: %s", descricao))
		}
	}

	if len(newDescriptions) > 0 {
		newInfo := strings.Join(newDescriptions, "\n")
		if lead.Description == "" {
			lead.Description = newInfo
		} else {
			lead.Description = fmt.Sprintf("%s\n%s", lead.Description, newInfo)
		}
	}

	if v, ok := cnpjDetails["atividade_principal"].(map[string]interface{}); ok {
		if descricao, ok := v["descricao"].(string); ok {
			lead.PrimaryActivity = descricao
		}
	}

	if v, ok := cnpjDetails["atividades_secundarias"].([]interface{}); ok {
		var secondaryActivities []string
		for _, sec := range v {
			if secMap, ok := sec.(map[string]interface{}); ok {
				if descricao, ok := secMap["descricao"].(string); ok {
					secondaryActivities = append(secondaryActivities, descricao)
				}
			}
		}

		lead.SecondaryActivities = strings.Join(secondaryActivities, ", ")
	}

	if v, ok := cnpjDetails["capital_social"].(string); ok {
		equityCapital, err := strconv.ParseFloat(v, 64)
		if err == nil {
			lead.EquityCapital = equityCapital
		} else {
			return fmt.Errorf("Erro ao converter capital_social: %v", err)
		}
	}

	if v, ok := cnpjDetails["data_inicio"].(string); ok {
		foundationDate, err := time.Parse("2006-01-02", v)
		if err == nil {
			lead.FoundationDate = sql.NullTime{Time: foundationDate, Valid: true}
		} else {
			return fmt.Errorf("Erro ao converter data de inicio: %v", err)
		}
	}

	if v, ok := cnpjDetails["email"].(string); ok {
		isValidEmail, err := validateEmail(v)
		if err != nil {
			log.Printf("Erro ao validar email: %v", err)
		} else if isValidEmail {
			if lead.Email != "" {
				lead.Email = fmt.Sprintf("%s; %s", lead.Email, v)
			} else {
				lead.Email = v
			}
		}
	}

	if socios, ok := cnpjDetails["socios"].([]interface{}); ok {
		var ownerDetails []string
		for _, socio := range socios {
			if socioMap, ok := socio.(map[string]interface{}); ok {
				nome, _ := socioMap["nome"].(string)
				qualificacao, _ := socioMap["qualificacao"].(string)

				if nome != "" && qualificacao != "" {
					ownerDetails = append(ownerDetails, fmt.Sprintf("%s (%s)", nome, qualificacao))
				}
			}
		}

		if len(ownerDetails) > 0 {

			lead.Owner = strings.Join(ownerDetails, ", ")
		}

		normalizePhone := func(phone string) string {
			re := regexp.MustCompile(`[\s\-\(\)]`)
			return re.ReplaceAllString(phone, "")
		}

		formatPhone := func(phone string) string {
			phone = normalizePhone(phone)
			if len(phone) == 11 {
				return fmt.Sprintf("+55 %s %s-%s", phone[:2], phone[2:6], phone[6:])
			}
			return phone
		}

		var newPhones []string

		if telefone1, ok := cnpjDetails["telefone1"].(string); ok && telefone1 != "" {
			hasWhatsapp, _ := hasWhatsApp(telefone1)
			formattedPhone := formatPhone(telefone1)
			newPhones = append(newPhones, formattedPhone)
			if hasWhatsapp {
				lead.Whatsapp += formattedPhone + ", "
			}
		}

		if telefone2, ok := cnpjDetails["telefone2"].(string); ok && telefone2 != "" {
			hasWhatsapp, _ := hasWhatsApp(telefone2)
			formattedPhone := formatPhone(telefone2)
			newPhones = append(newPhones, formattedPhone)
			if hasWhatsapp {
				lead.Whatsapp += formattedPhone + ", "
			}
		}

		if lead.Phone != "" {
			existingPhones := strings.Split(lead.Phone, ", ")
			normalizedExistingPhones := make(map[string]bool)

			for _, existingPhone := range existingPhones {
				normalizedExistingPhones[normalizePhone(existingPhone)] = true
			}

			for _, newPhone := range newPhones {
				if !normalizedExistingPhones[normalizePhone(newPhone)] {
					existingPhones = append(existingPhones, newPhone)
				}
			}

			lead.Phone = strings.Join(existingPhones, ", ")

		} else {

			lead.Phone = strings.Join(newPhones, ", ")
		}

		if v, ok := cnpjDetails["porte"].(string); ok {
			lead.CompanySize = v
		}

		var additionalInfo []string

		if mei, ok := cnpjDetails["mei"].(map[string]interface{}); ok {
			if optanteMei, ok := mei["optante_mei"].(string); ok && optanteMei != "" {
				additionalInfo = append(additionalInfo, fmt.Sprintf("Optante MEI: %s", optanteMei))
			}
			if dataOpcao, ok := mei["data_opcao"].(string); ok && dataOpcao != "" {
				additionalInfo = append(additionalInfo, fmt.Sprintf("Data de Opção MEI: %s", dataOpcao))
			}
			if dataExclusao, ok := mei["data_exclusao"].(string); ok && dataExclusao != "" {
				additionalInfo = append(additionalInfo, fmt.Sprintf("Data de Exclusão MEI: %s", dataExclusao))
			}
		}

		if naturezaJuridica, ok := cnpjDetails["natureza_juridica"].(string); ok && naturezaJuridica != "" {
			additionalInfo = append(additionalInfo, fmt.Sprintf("Natureza Jurídica: %s", naturezaJuridica))
		}

		if simples, ok := cnpjDetails["simples"].(map[string]interface{}); ok {
			if optanteSimples, ok := simples["optante_simples"].(string); ok && optanteSimples != "" {
				additionalInfo = append(additionalInfo, fmt.Sprintf("Optante Simples Nacional: %s", optanteSimples))
			}
			if dataOpcaoSimples, ok := simples["data_opcao"].(string); ok && dataOpcaoSimples != "" {
				additionalInfo = append(additionalInfo, fmt.Sprintf("Data de Opção Simples: %s", dataOpcaoSimples))
			}
			if dataExclusaoSimples, ok := simples["data_exclusao"].(string); ok && dataExclusaoSimples != "" {
				additionalInfo = append(additionalInfo, fmt.Sprintf("Data de Exclusão Simples: %s", dataExclusaoSimples))
			}
		}

		if situacao, ok := cnpjDetails["situacao"].(map[string]interface{}); ok {
			if situacaoNome, ok := situacao["nome"].(string); ok && situacaoNome != "" {
				additionalInfo = append(additionalInfo, fmt.Sprintf("Situação: %s", situacaoNome))
			}
			if situacaoData, ok := situacao["data"].(string); ok && situacaoData != "" {
				additionalInfo = append(additionalInfo, fmt.Sprintf("Data da Situação: %s", situacaoData))
			}
			if situacaoMotivo, ok := situacao["motivo"].(string); ok && situacaoMotivo != "" {
				additionalInfo = append(additionalInfo, fmt.Sprintf("Motivo da Situação: %s", situacaoMotivo))
			}
		}

	}

	err = db.UpdateLead(lead)
	if err != nil {
		log.Printf("Erro ao atualizar o lead: %v", err)
		return fmt.Errorf("Erro ao atualizar o lead: %v", err)
	}

	log.Printf("Lead atualizado com sucesso para ID: %s", leadID)
	return nil
}

func isTemporaryError(err error) bool {
	for _, tempErr := range temporaryErrors {
		if err == tempErr {
			log.Printf("Erro temporário detectado: %v", err)
			return true
		}
	}

	log.Printf("Erro irreversível: %v", err)
	return false
}

func leadHandler(w http.ResponseWriter, r *http.Request) {
	var lead db.Lead
	var lead_step db.LeadStep

	err := json.NewDecoder(r.Body).Decode(&lead)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	existingLead, err := db.GetLeadByGoogleId(lead.GoogleId)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		log.Printf("Erro ao buscar lead com Google ID: %s - %v", lead.GoogleId, err)
		http.Error(w, "Erro ao buscar lead", http.StatusInternalServerError)
		return
	}

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

func consumeLeadsFromRabbitMQ(ch *amqp.Channel) {
	q, err := ch.QueueDeclare(
		"leads_queue",
		true,  // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		log.Printf("Mensagens pendentes na fila %s: %d", q.Name, q.Messages)
		log.Fatalf("Failed to declare queue: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		false, // Auto-acknowledge
		false, // Exclusive
		false, // No-local
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}
	log.Println("Consumidor registrado com sucesso")

	go func() {
		for d := range msgs {
			log.Printf("Mensagem recebida: %s", d.Body)

			var leadData map[string]interface{}
			if err := json.Unmarshal(d.Body, &leadData); err != nil {
				log.Printf("Erro ao decodificar JSON: %v", err)
				d.Nack(false, true)
				continue
			}
			log.Println("Mensagem decodificada com sucesso")

			if err := saveLeadToDatabase(leadData); err != nil {
				log.Printf("Erro ao processar lead: %v", err)
				d.Nack(false, false)
				continue
			}
			log.Println("Mensagem processada com sucesso!")

			d.Ack(false)
		}
	}()

	log.Println("Consumindo leads do RabbitMQ...")
	select {} // Block para manter o consumer ativo
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
	if connection == nil {
		return nil, fmt.Errorf("Conexão com RabbitMQ não inicializada")
	}

	channel, err := connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("Erro ao abrir canal: %v", err)
	}

	// Lista de exchanges a serem declaradas
	exchanges := []string{"scrapper_exchange", "leads_exchange"}

	for _, exchangeName := range exchanges {
		err = channel.ExchangeDeclare(
			exchangeName,
			"fanout", // Tipo de exchange
			true,     // Durável
			false,    // Auto-delete
			false,    // Interno
			false,    // No-wait
			nil,      // Arguments
		)
		if err != nil {
			return nil, fmt.Errorf("Erro ao declarar exchange %s: %v", exchangeName, err)
		}
		log.Printf("Exchange %s declarada com sucesso!", exchangeName)
	}

	return channel, nil
}
