package db


type Lead struct {
	ID               uint    `gorm:"primaryKey"`
	Name             string  `gorm:"size:255"`
	Address          string  `gorm:"type:text"`
	City          string  `gorm:"type:text"`
	State          string  `gorm:"type:text"`
	Country          string  `gorm:"type:text"`
	Owner          string  `gorm:"type:text"`
	Source          string  `gorm:"type:text"`
	Phone            string  `gorm:"size:50"`
	Whatsapp            string  `gorm:"size:50"`
	Website          string  `gorm:"type:text"`
	Email          	 string  `gorm:"type:text"`
	Instagram          	 string  `gorm:"type:text"`
	Facebook          	 string  `gorm:"type:text"`
	TikTok          	 string  `gorm:"type:text"`
	CNPJ          	 string  `gorm:"type:text"`
	Rating           float64 `gorm:"type:numeric"`
	Quality          string  `gorm:"size:50"`
	FieldsFilled     int     `gorm:"default:0"`
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
