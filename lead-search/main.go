package main

import (
	"encoding/json"
	"fmt"
	"strconv"

	"log"
	"net/http"
	"os"
	"time"

	"lead-search/googleplaces"
	"lead-search/repository"

	"database/sql"

	_ "github.com/mattn/go-sqlite3"
	"github.com/streadway/amqp"

	"github.com/joho/godotenv"
)



func main() {

    log.Println("Starting the service...")

	conn, ch, err := connectToRabbitMQ()
	if err != nil {
		log.Fatalf("Erro ao conectar ao RabbitMQ: %v", err)
	}
	defer conn.Close()
	defer ch.Close()


    err = godotenv.Load()
    if err != nil {
        log.Fatal("Error loading .env file")
    }
    log.Println(".env file loaded successfully")

	apiKey := os.Getenv("GOOGLE_PLACES_API_KEY")

    if apiKey ==  "" {
        log.Fatal("API key is required. Set the GOOGLE_PLACES_API_KEY environment variable.")
    }

    db, err := setupDatabase()
	if err != nil {
		log.Fatalf("Erro ao configurar o banco de dados: %v", err)
	}
	defer db.Close()

 
    http.HandleFunc("/start-search", func(w http.ResponseWriter, r *http.Request) {
        startSearchHandler(w, r, db, ch)
    })

    log.Println("Starting server on port 8082...")
	err = http.ListenAndServe(":8082", nil)
	if err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
   
}


func connectToRabbitMQ() (*amqp.Connection, *amqp.Channel, error) {
    log.Println("Conectando ao RabbitMQ...")
    rabbitmqHost := os.Getenv("RABBITMQ_HOST")
    rabbitmqPort := os.Getenv("RABBITMQ_PORT")

    
    if rabbitmqHost == "" || rabbitmqPort == "" {
        return nil, nil, fmt.Errorf("RABBITMQ_HOST and RABBITMQ_PORT must be set")
    }

    log.Printf("Conectado ao RabbitMQ em %s:%s", rabbitmqHost, rabbitmqPort)

    var conn *amqp.Connection
    var ch *amqp.Channel
    var err error

   
    for i := 0; i < 5; i++ {
        conn, err = amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%s/", rabbitmqHost, rabbitmqPort))
        if err == nil {
            
            ch, err = conn.Channel()
            if err != nil {
                return nil, nil, fmt.Errorf("failed to open RabbitMQ channel: %v", err)
            }
            return conn, ch, nil
        }

        
        log.Printf("Failed to connect to RabbitMQ at %s:%s, retrying in 10 seconds... (%d/5)", rabbitmqHost, rabbitmqPort, i+1)
        time.Sleep(10 * time.Second)
    }

    
    return nil, nil, fmt.Errorf("failed to connect to RabbitMQ after retries: %v", err)
}


func startSearchHandler(w http.ResponseWriter, r *http.Request, db *sql.DB, ch *amqp.Channel) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	categoryID := r.URL.Query().Get("category_id")
	zipcodeIDString := r.URL.Query().Get("zipcode_id")
	radius := r.URL.Query().Get("radius")


	if categoryID == "" || zipcodeIDString == "" || radius == "" {
		http.Error(w, "Missing required parameters", http.StatusBadRequest)
		return
	}

	radiusInt, err := strconv.Atoi(radius)
	if err != nil {
		http.Error(w, "Invalid radius value", http.StatusBadRequest)
		return
	}

    zipcodeID, err := strconv.Atoi(zipcodeIDString)
    if err != nil {
	http.Error(w, "Invalid zipcode_id value", http.StatusBadRequest)
	return
    }
 
    err = startSearch(categoryID, zipcodeID, radiusInt, db, ch)
    if err != nil {
    http.Error(w, fmt.Sprintf("Failed to start search: %v", err), http.StatusInternalServerError)
    return
}

	fmt.Fprintf(w, "Search started for categoryID: %s, zipcodeID: %d, radius: %d", categoryID, zipcodeID, radiusInt)
	
	
}


func setupDatabase() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", "./geo.db")
	if err != nil {
		return nil, err
	}

	// Create tables
	createTablesSQL := `
		CREATE TABLE IF NOT EXISTS country (
			id INTEGER PRIMARY KEY AUTOINCREMENT, 
			name TEXT
		);

		CREATE TABLE IF NOT EXISTS state (
			id INTEGER PRIMARY KEY AUTOINCREMENT, 
			name TEXT, 
			country_id INTEGER, 
			status INTEGER DEFAULT 0, -- campo de status
			FOREIGN KEY(country_id) REFERENCES country(id)
		);

		CREATE TABLE IF NOT EXISTS city (
			id INTEGER PRIMARY KEY AUTOINCREMENT, 
			name TEXT, 
			state_id INTEGER, 
			status INTEGER DEFAULT 0, -- campo de status
			FOREIGN KEY(state_id) REFERENCES state(id)
		);

		CREATE TABLE IF NOT EXISTS district (
			id INTEGER PRIMARY KEY AUTOINCREMENT, 
			name TEXT, 
			city_id INTEGER, 
			status INTEGER DEFAULT 0, -- campo de status
			FOREIGN KEY(city_id) REFERENCES city(id)
		);

		CREATE TABLE IF NOT EXISTS zipcode (
			id INTEGER PRIMARY KEY AUTOINCREMENT, 
			start_zip TEXT, 
			end_zip TEXT, 
			district_id INTEGER, 
			status INTEGER DEFAULT 0, -- campo de status
			FOREIGN KEY(district_id) REFERENCES district(id)
		);

		CREATE TABLE IF NOT EXISTS radius (
			id INTEGER PRIMARY KEY AUTOINCREMENT, 
			radius INTEGER
		);

		CREATE TABLE IF NOT EXISTS categoria (
			id INTEGER PRIMARY KEY AUTOINCREMENT, 
			nome TEXT, 
			status INTEGER DEFAULT 0 -- campo de status
		);

		CREATE TABLE IF NOT EXISTS search_progress (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			categoria_id INTEGER,
			country_id INTEGER,
			state_id INTEGER,
			city_id INTEGER,
			district_id INTEGER,
			zipcode_id INTEGER,
			search_done INTEGER DEFAULT 0, -- campo para indicar se a pesquisa foi concluída
			search_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- data de criação
			FOREIGN KEY(categoria_id) REFERENCES categoria(id),
			FOREIGN KEY(country_id) REFERENCES country(id),
			FOREIGN KEY(state_id) REFERENCES state(id),
			FOREIGN KEY(city_id) REFERENCES city(id),
			FOREIGN KEY(district_id) REFERENCES district(id),
			FOREIGN KEY(zipcode_id) REFERENCES zipcode(id)
		);
	`

	_, err = db.Exec(createTablesSQL)
	if err != nil {
		return nil, err
	}

	log.Println("Database setup completed")
	return db, nil
}


func startSearch(categoryID string,  zipcodeID int, radius int, db *sql.DB, ch *amqp.Channel) error {
	apiKey := os.Getenv("GOOGLE_PLACES_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("API key is required. Set the GOOGLE_PLACES_API_KEY environment variable.")
	}

    categoryName, err := repository.GetCategoryNameByID(db, categoryID)
	if err != nil {
		return fmt.Errorf("Failed to get category name: %v", err)
	}
  

	locationInfo, err := repository.GetLocationInfoByZipcodeID(db, zipcodeID)
	if err != nil {
		return fmt.Errorf("Failed to get location info by zipcode: %v", err)
	}

   

    startZip, err := repository.GetFirstZipCodeInRange(db, zipcodeID)
    if err != nil {
        return fmt.Errorf("Failed to get location info by zipcode: %v", err)
    }

	progress := repository.SearchProgress{
		CategoriaID: categoryID,
		CountryID:   locationInfo.CountryID,
		StateID:     locationInfo.StateID,
		CityID:      locationInfo.CityID,
		DistrictID:  locationInfo.DistrictID,
		ZipcodeID:   locationInfo.ZipcodeID,
		Radius:      radius,
		SearchDone:  0, 
	}

	progressID, err := repository.InsertSearchProgress(db, progress)
	if err != nil {
		return fmt.Errorf("Failed to insert search progress: %v", err)
	}

    cityName, err := repository.GetCityNameByID(db, locationInfo.CityID)
    if err != nil {
        return fmt.Errorf("Failed to get city name: %v", err)
    }

	log.Printf("Search progress inserted with ID: %d", progressID)

	maxPages := 1

    service := googleplaces.NewService(apiKey)

    coordinates, err := service.GeocodeZip(startZip)
    if err != nil {
        log.Fatalf("Failed to get coordinates for city: %v", err)
    }


	for currentPage := 1; currentPage <= maxPages; currentPage++ {
		log.Printf("Iniciando busca na página %d para a categoria %s na cidade %s", currentPage, categoryName, locationInfo.CityName)

		placeDetailsFromSearch,  err := service.SearchPlaces(categoryName, coordinates, radius,maxPages)
		if err != nil {
			return fmt.Errorf("Error fetching places: %v", err)
		}

		
		for _, place := range placeDetailsFromSearch {
			placeID := place["PlaceID"].(string)
			placeDetails, err := service.GetPlaceDetails(placeID)
			if err != nil {
				log.Printf("Failed to get details for place ID %s: %v", placeID, err)
				continue
			}

			placeDetails["Category"] = categoryName
			placeDetails["City"] = cityName
			placeDetails["Radius"] = radius

			err = publishLeadToRabbitMQ(ch, placeDetails)
			if err != nil {
				log.Printf("Failed to publish lead to RabbitMQ: %v", err)
			}
		}

		
		err = repository.UpdateSearchProgressPage(db, progressID, currentPage)
		if err != nil {
			return fmt.Errorf("Failed to update search progress: %v", err)
		}

		log.Printf("Search progress updated: page %d completed", currentPage)

	
	}

	return nil
}

func getFirstZipCodeInRange(db *sql.DB, districtID int) (string, error) {
	var startZip string
	err := db.QueryRow("SELECT start_zip FROM zipcode WHERE district_id = ?", districtID).Scan(&startZip)
	if err != nil {
		return "", err
	}
	return startZip, nil
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



func sendLeadToRabbitMQ(ch *amqp.Channel, details map[string]interface{}) error {
    
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
        

        return fmt.Errorf("Failed to publish a message to RabbitMQ: %v", err)
    }

    log.Printf("Lead sent to RabbitMQ for later processing: %s", leadData)
    return nil
}