package db

import (
	"fmt"
	"log"
	

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var DB *gorm.DB


func Connect() error{
	var err error
	dsn := "host=db user=postgres password=postgres dbname=leadsdb port=5432 sslmode=disable"
	DB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Falha ao conectar ao banco de dados:", err)
	}
	fmt.Println("Conexão com o banco de dados bem-sucedida.")
	return nil
}



func Close() {
	sqlDB, err := DB.DB()
	if err != nil {
		log.Fatal("Erro ao fechar a conexão com o banco de dados:", err)
	}
	sqlDB.Close()
}

