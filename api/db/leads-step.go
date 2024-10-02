package db

import (
	"time"
	"github.com/google/uuid"
)


type LeadStep struct {
	ID        uuid.UUID `gorm:"type:uuid;default:uuid_generate_v4();primaryKey" json:"id"`
	LeadID    uuid.UUID `gorm:"type:uuid" json:"lead_id"` 
	Step      string    `gorm:"type:text" json:"step"`    
	Status    string    `gorm:"type:text" json:"status"`  
	Timestamp time.Time `gorm:"autoCreateTime" json:"timestamp"`
	Details   string    `gorm:"type:text" json:"details"` 
}


func CreateLeadStep(leadStep *LeadStep) error {
	result := DB.Create(leadStep)
	if result.Error != nil {
		return result.Error
	}
	return nil
}


func GetLeadSteps(leadID uuid.UUID) ([]LeadStep, error) {
	var steps []LeadStep
	result := DB.Where("lead_id = ?", leadID).Find(&steps)
	if result.Error != nil {
		return nil, result.Error
	}
	return steps, nil
}


