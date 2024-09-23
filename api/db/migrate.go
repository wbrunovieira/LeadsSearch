package db

import ("gorm.io/gorm"
		"log"
)


func Migrate(db *gorm.DB) {

	err := db.Exec("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"").Error
    if err != nil {
        log.Fatalf("Falha ao criar a extensão uuid-ossp: %v", err)
    }
	err = DB.AutoMigrate(&Lead{})
	if err != nil {
		panic("Falha ao migrar banco de dados: " + err.Error())
	}
}
