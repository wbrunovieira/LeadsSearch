package db

import (
	"database/sql"
)

type Lead struct {
	ID               uint    `gorm:"primaryKey"`
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
	PriceLevel           float64 `gorm:"type:numeric"`
	UserRatingsTotal           int     `gorm:"default:0"`

	CompanySize        string    `gorm:"size:50"`
	Revenue                 float64   `gorm:"type:numeric"`
	EmployeesCount          int       `gorm:"default:0"`   
	Description             string    `gorm:"type:text"`   
	PrimaryActivity         string    `gorm:"type:text"`

	Quality          string  `gorm:"size:50"`
	SearchTerm          string  `gorm:"size:50"`
	FieldsFilled     int     `gorm:"default:0"`
	GoogleId    	 string  `gorm:"type:text"`
	CreatedAt        string  `gorm:"autoCreateTime"`
}



func CreateLead(lead *Lead) error {
	return DB.Create(lead).Error
}


func GetLeads() ([]Lead, error) {
	var leads []Lead
	result := DB.Find(&leads)
	return leads, result.Error
}
