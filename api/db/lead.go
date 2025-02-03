package db

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Lead struct {
	ID                    uuid.UUID    `gorm:"type:uuid;default:uuid_generate_v4();primaryKey" json:"id"`
	BusinessName          string       `gorm:"size:255"`
	RegisteredName        string       `gorm:"size:255"`
	FoundationDate        sql.NullTime `gorm:"type:date"`
	Address               string       `gorm:"type:text"`
	City                  string       `gorm:"type:text"`
	State                 string       `gorm:"type:text"`
	Country               string       `gorm:"type:text"`
	ZIPCode               string       `gorm:"type:text"`
	Owner                 string       `gorm:"type:text"`
	Source                string       `gorm:"type:text"`
	Phone                 string       `gorm:"size:50"`
	Whatsapp              string       `gorm:"size:50"`
	Website               string       `gorm:"type:text"`
	Email                 string       `gorm:"type:text"`
	LeadSteps             []LeadStep   `gorm:"foreignKey:LeadID"`
	Instagram             string       `gorm:"type:text"`
	Facebook              string       `gorm:"type:text"`
	TikTok                string       `gorm:"type:text"`
	CompanyRegistrationID string       `gorm:"type:text"`
	Categories            string       `gorm:"type:text"`
	Rating                float64      `gorm:"type:numeric"`
	PriceLevel            int          `gorm:"default:0"`
	UserRatingsTotal      int          `gorm:"default:0"`
	Vicinity              string       `gorm:"type:text"`
	PermanentlyClosed     bool         `gorm:"default:false"`

	CompanySize    string  `gorm:"size:50"`
	Revenue        float64 `gorm:"type:numeric"`
	EmployeesCount int     `gorm:"default:0"`
	Description    string  `gorm:"type:text"`

	PrimaryActivity     string  `gorm:"type:text"`
	SecondaryActivities string  `gorm:"type:text"`
	Types               string  `gorm:"type:text"`
	EquityCapital       float64 `gorm:"type:numeric"`

	BusinessStatus string `gorm:"type:text"`

	Quality      string `gorm:"size:50"`
	SearchTerm   string `gorm:"size:50"`
	FieldsFilled int    `gorm:"default:0"`
	GoogleId     string `gorm:"type:text"`

	CreatedAt time.Time `gorm:"autoCreateTime"`
	UpdatedAt time.Time `gorm:"autoUpdateTime"`
}

func CreateLead(lead *Lead) error {
	var existingLead Lead
	result := DB.Where("google_id = ?", lead.GoogleId).First(&existingLead)
	if result.Error == nil {
		return fmt.Errorf("Lead com GoogleId %s já existe", lead.GoogleId)
	}

	if result.Error != nil && result.Error != gorm.ErrRecordNotFound {

		return fmt.Errorf("Erro ao verificar se o lead já existe: %v", result.Error)
	}

	result = DB.Create(lead)
	if result.Error != nil {
		return fmt.Errorf("Failed to save lead to database: %v", result.Error)
	}
	log.Printf("Salvando lead no  funcao CreateLead PostgreSQL: Nome=%s, WhatsApp=%s", lead.BusinessName, lead.Whatsapp)

	leadStep := LeadStep{
		LeadID:  lead.ID,
		Step:    "Lead Criado",
		Status:  "Sucesso",
		Details: fmt.Sprintf("Lead %s foi criado com sucesso", lead.BusinessName),
	}
	err := CreateLeadStep(&leadStep)
	if err != nil {
		return fmt.Errorf("Falha ao salvar o passo do lead: %v", err)
	}

	return nil
}

func GetLeads() ([]Lead, error) {
	var leads []Lead
	result := DB.Find(&leads)
	if result.Error != nil {
		return nil, result.Error
	}
	return leads, nil
}

func GetLeadByGoogleId(googleId string) (*Lead, error) {
	var lead Lead
	result := DB.Where("google_id = ?", googleId).First(&lead)
	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			return nil, fmt.Errorf("Lead não encontrado")
		}
		return nil, result.Error
	}
	return &lead, nil
}

func GetLeadIdByGoogleId(googleId string) (uuid.UUID, error) {
	var lead Lead
	result := DB.Select("id").Where("google_id = ?", googleId).First(&lead)
	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			return uuid.Nil, fmt.Errorf("Lead não encontrado para o Google ID: %s", googleId)
		}
		return uuid.Nil, result.Error
	}
	return lead.ID, nil
}

func GetLeadByID(leadID uuid.UUID) (*Lead, error) {
	var lead Lead
	result := DB.First(&lead, "id = ?", leadID)

	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}

	return &lead, nil
}

func UpdateLead(lead *Lead) error {
	existingLead, err := GetLeadByID(lead.ID)
	if err != nil {
		return fmt.Errorf("Erro ao buscar o lead: %v", err)
	}

	if existingLead == nil {
		return fmt.Errorf("Lead não encontrado para ID: %s", lead.ID)
	}

	if lead.Description != "" {
		if existingLead.Description != "" {
			existingLead.Description = fmt.Sprintf("%s\n%s", existingLead.Description, lead.Description)
		} else {
			existingLead.Description = lead.Description
		}
	}

	result := DB.Save(existingLead)
	if result.Error != nil {
		return fmt.Errorf("Erro ao atualizar o lead: %v", result.Error)
	}
	return nil
}
