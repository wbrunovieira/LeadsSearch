package main

import (
	"api/db"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/wbrunovieira/ProtoDefinitionsLeadsSearch/leadpb"

	"time"
)

type LeadServer struct {
	leadpb.UnimplementedLeadServiceServer
}

func (s *LeadServer) SendLead(ctx context.Context, req *leadpb.LeadRequest) (*leadpb.LeadResponse, error) {

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

	// Inserir no banco de dados
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

func main() {
	
	db.Connect()
	defer db.Close()

	
	db.Migrate()

	
	http.HandleFunc("/leads", leadHandler)

	
	port := os.Getenv("PORT")
	fmt.Println("API rodando na porta", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
