package db

import (
		"log"
)


func Migrate() error {

	err := DB.Exec("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"").Error
    if err != nil {
        log.Fatalf("Falha ao criar a extensão uuid-ossp: %v", err)
    }

	err = DB.AutoMigrate(&Lead{})
	if err != nil {
		panic("Falha ao migrar banco de dados: " + err.Error())
	}

	err = DB.AutoMigrate(&LeadStep{})
	if err != nil {
		panic("Falha ao migrar banco de dados: " + err.Error())
	}
	return nil
}
