package db

import (
	"time"
	"github.com/google/uuid"
)

// LeadStep representa cada etapa ou ação feita no lead
type LeadStep struct {
	ID        uuid.UUID `gorm:"type:uuid;default:uuid_generate_v4();primaryKey" json:"id"`
	LeadID    uuid.UUID `gorm:"type:uuid" json:"lead_id"` // Chave estrangeira para a tabela Leads
	Step      string    `gorm:"type:text" json:"step"`    // Nome da etapa (ex: "Criado", "CNPJ Buscado", etc.)
	Status    string    `gorm:"type:text" json:"status"`  // Status da etapa (ex: "Sucesso", "Erro")
	Timestamp time.Time `gorm:"autoCreateTime" json:"timestamp"`
	Details   string    `gorm:"type:text" json:"details"` // Detalhes adicionais sobre a etapa
}

// CreateLeadStep cria um novo registro de passo/etapa no banco de dados
func CreateLeadStep(leadStep *LeadStep) error {
	result := DB.Create(leadStep)
	if result.Error != nil {
		return result.Error
	}
	return nil
}

// GetLeadSteps retorna todos os passos de um lead específico
func GetLeadSteps(leadID uuid.UUID) ([]LeadStep, error) {
	var steps []LeadStep
	result := DB.Where("lead_id = ?", leadID).Find(&steps)
	if result.Error != nil {
		return nil, result.Error
	}
	return steps, nil
}
