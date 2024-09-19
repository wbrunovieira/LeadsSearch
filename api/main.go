package main

import (
	"api/db"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
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

func main() {
	
	db.Connect()
	defer db.Close()

	
	db.Migrate()

	
	http.HandleFunc("/leads", leadHandler)

	
	port := os.Getenv("PORT")
	fmt.Println("API rodando na porta", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
