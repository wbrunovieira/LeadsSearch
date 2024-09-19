package db


func Migrate() {
	err := DB.AutoMigrate(&Lead{})
	if err != nil {
		panic("Falha ao migrar banco de dados: " + err.Error())
	}
}
