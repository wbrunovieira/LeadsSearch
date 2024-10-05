package main

import (
	"api/db"
	"bytes"
	"database/sql"
	"io/ioutil"
	"regexp"
	"strconv"

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


var rabbitConn *amqp.Connection
var rabbitChannel *amqp.Channel

var temporaryErrors = []error{
    sql.ErrConnDone,
    sql.ErrTxDone,
    
}

func main() {

	log.Println("Starting API service...")

    _, channel, err := connectToRabbitMQ() 
	if err != nil {
		log.Fatalf("Erro ao conectar ao RabbitMQ: %v", err)
	}
	defer closeRabbitMQ()

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
 	go consumeLeadsFromRabbitMQ(channel)

    log.Println("Starting to consume companies from RabbitMQ...")
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

    select {}
}

func hasWhatsApp(phone string) (bool, error) {
	url := "http://validator-service:8090/check_whatsapp"
	client := &http.Client{}

	payload := fmt.Sprintf(`["%s"]`, phone)
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
		return false, fmt.Errorf("Erro na requisição para verificar WhatsApp: %s", response.Status)
	}

    body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return false, err
	}

	var result []map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return false, err
	}

	if len(result) > 0 && result[0]["has_whatsapp"].(bool) {
		return true, nil
	}

	return false, nil
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
    body, err := ioutil.ReadAll(response.Body)
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



func connectToRabbitMQ() (*amqp.Connection, *amqp.Channel, error) {
    rabbitmqHost := os.Getenv("RABBITMQ_HOST")
    rabbitmqPort := os.Getenv("RABBITMQ_PORT")

    if rabbitConn != nil && rabbitChannel != nil {
        log.Println("RabbitMQ já está conectado")
        return rabbitConn, rabbitChannel, nil
    }


     if rabbitmqHost == "" || rabbitmqPort == "" {
        return nil, nil, fmt.Errorf("RABBITMQ_HOST and RABBITMQ_PORT must be set")
    }

	var conn *amqp.Connection
    var ch *amqp.Channel
    var err error

    log.Printf("Tentando conectar ao RabbitMQ: %s:%s", rabbitmqHost, rabbitmqPort)

   for i := 0; i < 5; i++ {
        conn, err = amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%s/", rabbitmqHost, rabbitmqPort))
        if err == nil {
            log.Println("Conexão com RabbitMQ bem-sucedida")
            break
        }
        log.Printf("Failed to connect to RabbitMQ at %s:%s, retrying in 10 seconds... (%d/5)", rabbitmqHost, rabbitmqPort, i+1)
        time.Sleep(10 * time.Second)
    }

    if err != nil {
        return nil, nil, fmt.Errorf("failed to connect to RabbitMQ after 5 retries: %v", err)
    }

   
    ch, err = conn.Channel()
    if err != nil {
        return nil, nil, fmt.Errorf("failed to open RabbitMQ channel: %v", err)
    }

    err = ch.Qos(10, 0, false) 
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to set QoS: %v", err)
	}


    rabbitConn = conn
    rabbitChannel = ch

    return conn, ch, nil

}

func ConnectToRedis() error{
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
		false,  // Manual ack
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

            log.Printf("Mensagem recebida do RabbitMQ pelo consumeCompaniesFromRabbitMQ: %s", string(d.Body))

			var combinedData map[string]interface{}

			err := json.Unmarshal(d.Body, &combinedData)
			if err != nil {
				log.Printf("Erro ao decodificar JSON: %v", err)
                d.Nack(false, true)  
                continue
			}

			companiesInfo, ok := combinedData["companies_info"].([]interface{})
			if !ok {
				log.Printf("companies_info não encontrado na mensagem: %v", combinedData)
				d.Nack(false, true)  
                continue
			}

			
			var processingError error

			
			for _, companyInfo := range companiesInfo {
				companyMap, ok := companyInfo.(map[string]interface{})
				if !ok {
					log.Printf("Erro ao converter companyInfo para map: %v", companyInfo)
					processingError = fmt.Errorf("erro ao converter companyInfo")
                    break  
				}
                log.Printf("companyMap aqui: %v", companyMap)
                				
                leadID, err := saveCompanyData(companyMap)
                if err != nil {
                    log.Printf("Erro ao atualizar lead saveCompanyData: %v", err)

                    if isTemporaryError(err) { 
                        d.Nack(false, true)  
                        log.Printf("Erro temporário. Mensagem reenfileirada para nova tentativa.")
                    } else {
                        d.Nack(false, false)  
                        log.Printf("Erro irreversível. Mensagem descartada.")
                    }
                    processingError = err  
                    break  
                }

				
				cnpjDetails, ok := combinedData["cnpj_details"].([]interface{})
				if !ok {
					log.Printf("cnpj_details não encontrado ou não é uma lista válida: %v", combinedData)
					processingError = fmt.Errorf("cnpj_details não encontrado")
					break  
				}

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

					log.Printf("Detalhes do CNPJ:\n%s", string(cnpjDetailJSON))

					err = updateLeadWithCNPJDetailsByID(leadID, cnpjMap)
					if err != nil {
						log.Printf("Erro ao atualizar o lead: %v", err)
						if isTemporaryError(err) { 
							processingError = err 
							log.Printf("Erro temporário. Reenfileirando mensagem.")
							break
						} else {
							processingError = err  
							log.Printf("Erro irreversível. Descartando mensagem.")
							break
						}
					}
				}
			}

			
			if processingError != nil {
				continue  
			}

			
			err = d.Ack(false)
			if err != nil {
				log.Printf("Erro ao enviar ACK: %v", err)
			}
		}
	}()

	log.Println("Consumindo empresas do RabbitMQ...")
	select {} // Block para manter o consumer ativo
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
		hasWhatsapp, err := hasWhatsApp(v)
		if err != nil {
			log.Printf("Erro ao verificar WhatsApp: %v", err)
		} else if hasWhatsapp {
			lead.Whatsapp = v
		}
		lead.Phone = v
	}

    if v, ok := data["Email"].(string); ok {
        isValidEmail, err := validateEmail(v)
        if err != nil {
            log.Printf("Erro ao validar email: %v", err)
        } else if isValidEmail {
            lead.Email = v
        } else {
            log.Printf("Email inválido, não será salvo: %s", v)
        }
    }

  
    if v, ok := data["Website"].(string); ok {
        lead.Website = v
        
        if strings.HasPrefix(lead.Website, "https://www.instagram.com") {
            lead.Instagram = lead.Website
            lead.Website = ""
        }
        if strings.HasPrefix(lead.Website, "https://www.facebook.com.com") {
            lead.Facebook = lead.Website
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
	exchangeName := "leads_exchange"
    queueName := "leads_queue"

    args := amqp.Table{
		"x-dead-letter-exchange": "dlx_exchange", // Enviar para Dead Letter Exchange em caso de falha
		"x-message-ttl":          60000,          // TTL de 60 segundos
	}

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
        args,   // Arguments
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
        false,  // Auto-acknowledge
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

            if json.Valid(d.Body) {
				err := json.Unmarshal(d.Body, &leadData)
                if err != nil {
                    log.Printf("Erro ao decodificar JSON: %v", err)
                    d.Nack(false, true) 
                    continue
                }

                log.Printf("Processando lead: %v", leadData)
                err = saveLeadToDatabase(leadData)
                if err != nil {
                    log.Printf("Erro ao processar lead: %v", err)                   
                    
                    if isTemporaryError(err) { 
                        d.Nack(false, true)  
                        log.Printf("Erro temporário. Mensagem reenfileirada para nova tentativa.saveLeadToDatabase")
                    } else {
                        d.Nack(false, false) 
                        log.Printf("Erro irrecuperável. Mensagem descartada.saveLeadToDatabase")
                    }
                    continue
                }

                err = d.Ack(false)
                if err != nil {
                    log.Printf("Erro ao enviar ACK: %v", err)
                }  
				
			} 


        }
    }()

    log.Println("Consuming leads from RabbitMQ...")
    select {} // Block forever
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

    
    exchangeName := "scrapper_exchange"
    err = channel.ExchangeDeclare(
        exchangeName,
        "fanout",     
        true,         
        false,        
        false,        
        false,        
        nil,          
    )
    if err != nil {
        return nil, fmt.Errorf("Erro ao declarar exchange: %v", err)
    }

    log.Printf("Exchange %s declarada com sucesso", exchangeName)
    return channel, nil
}


