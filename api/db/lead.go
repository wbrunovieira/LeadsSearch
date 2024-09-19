package db


type Lead struct {
	ID               uint    `gorm:"primaryKey"`
	Nome             string  `gorm:"size:255"`
	Endereco         string  `gorm:"type:text"`
	Telefone         string  `gorm:"size:50"`
	Site             string  `gorm:"type:text"`
	Avaliacao        float64 `gorm:"type:numeric"`
	Qualidade        string  `gorm:"size:50"`
	CamposPreenchidos int    `gorm:"default:0"`
	CriadoEm         string  `gorm:"autoCreateTime"`
}


func CreateLead(lead *Lead) error {
	return DB.Create(lead).Error
}


func GetLeads() ([]Lead, error) {
	var leads []Lead
	result := DB.Find(&leads)
	return leads, result.Error
}
