package db

import (
	"database/sql"
	"time"
	"fmt"
	"github.com/google/uuid"
)

type Lead struct {
	ID uuid.UUID  `gorm:"type:uuid;default:uuid_generate_v4();primaryKey" json:"id"`
	BusinessName             string  `gorm:"size:255"`
	RegisteredName             string  `gorm:"size:255"`
	FoundationDate             sql.NullTime  `gorm:"type:date"`
	Address          string  `gorm:"type:text"`
	City          string  `gorm:"type:text"`
	State          string  `gorm:"type:text"`
	Country          string  `gorm:"type:text"`
	ZIPCode           string  `gorm:"type:text"`
	Owner          string  `gorm:"type:text"`
	Source          string  `gorm:"type:text"`
	Phone            string  `gorm:"size:50"`
	Whatsapp            string  `gorm:"size:50"`
	Website          string  `gorm:"type:text"`
	Email          	 string  `gorm:"type:text"`
	Instagram          	 string  `gorm:"type:text"`
	Facebook          	 string  `gorm:"type:text"`
	TikTok          	 string  `gorm:"type:text"`
	CompanyRegistrationID          	 string  `gorm:"type:text"`
	Rating           float64 `gorm:"type:numeric"`
	PriceLevel int `gorm:"default:0"`
	UserRatingsTotal           int     `gorm:"default:0"`
	Vicinity           string    `gorm:"type:text"`
	PermanentlyClosed  bool      `gorm:"default:false"`

	CompanySize        string    `gorm:"size:50"`
	Revenue                 float64   `gorm:"type:numeric"`
	EmployeesCount          int       `gorm:"default:0"`   
	Description             string    `gorm:"type:text"`   
	PrimaryActivity         string    `gorm:"type:text"`
	Types              string    `gorm:"type:text"`
	BusinessStatus     string    `gorm:"type:text"`

	Quality          string  `gorm:"size:50"`
	SearchTerm          string  `gorm:"size:50"`
	FieldsFilled     int     `gorm:"default:0"`
	GoogleId    	 string  `gorm:"type:text"`
	
	CreatedAt             time.Time      `gorm:"autoCreateTime"`
    UpdatedAt             time.Time      `gorm:"autoUpdateTime"`
}



func CreateLead(lead *Lead) error {
    result := DB.Create(lead)
    if result.Error != nil {
        return fmt.Errorf("Failed to save lead to database: %v", result.Error)
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

