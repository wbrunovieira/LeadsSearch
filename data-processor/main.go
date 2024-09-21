package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"

	_ "github.com/lib/pq"
)

var db *sql.DB

type Lead struct {
	ID               int
	Name             string
	Address          string
	Phone            string
	Website          string
	Quality          string
	FieldsFilled     int
}


func connectDB() {
	var err error
	connStr := "user=postgres password=postgres dbname=leadsdb host=db sslmode=disable"
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Error connecting to the database: %v", err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatalf("Error pinging the database: %v", err)
	}
	fmt.Println("Successfully connected to the database.")
}


// func evaluateQuality() {
// 	rows, err := db.Query("SELECT id, nome, endereco, telefone, site FROM leads WHERE qualidade IS NULL")
// 	if err != nil {
// 		log.Fatalf("Error fetching leads: %v", err)
// 	}
// 	defer rows.Close()

// 	for rows.Next() {
// 		var lead Lead
// 		err := rows.Scan(&lead.ID, &lead.Name, &lead.Address, &lead.Phone, &lead.Website)
// 		if err != nil {
// 			log.Fatalf("Error reading leads: %v", err)
// 		}

		
// 		lead.FieldsFilled = 0
// 		if lead.Name != "" {
// 			lead.FieldsFilled++
// 		}
// 		if lead.Address != "" {
// 			lead.FieldsFilled++
// 		}
// 		if lead.Phone != "" {
// 			lead.FieldsFilled++
// 		}
// 		if lead.Website != "" {
// 			lead.FieldsFilled++
// 		}

		
// 		if lead.FieldsFilled == 4 {
// 			lead.Quality = "good"
// 		} else {
// 			lead.Quality = "bad"
// 		}

		
// 		_, err = db.Exec("UPDATE leads SET qualidade = $1, campos_preenchidos = $2 WHERE id = $3",
// 			lead.Quality, lead.FieldsFilled, lead.ID)
// 		if err != nil {
// 			log.Printf("Error updating lead %d: %v", lead.ID, err)
// 		}
// 	}

// 	if err = rows.Err(); err != nil {
// 		log.Fatalf("Error iterating over leads: %v", err)
// 	}

// 	fmt.Println("Lead quality updated successfully.")
// }

func main() {
	
	connectDB()
	defer db.Close()

	
	// evaluateQuality()

	
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	fmt.Println("Data processor running on port", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
