package cnpjsearch

import (
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/PuerkitoBio/goquery"
)


func FetchCompanyDetails(cnpj string) {
	
	url := fmt.Sprintf("https://cnpj.biz/%s", cnpj)

	
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatalf("Failed to create request: %v", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36")

	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Failed to fetch the URL: %v", err)
	}
	defer resp.Body.Close()

	
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		log.Fatalf("Failed to parse the page: %v", err)
	}

	
	cnpjInfo := extractData(doc, "CNPJ", "p:contains('CNPJ')")
	razaoSocial := extractData(doc, "Razão Social", "p:contains('Razão Social')")
	nomeFantasia := extractData(doc, "Nome Fantasia", "p:contains('Nome Fantasia')")
	dataAbertura := extractData(doc, "Data da Abertura", "p:contains('Data de Abertura')")
	telefone := extractData(doc, "Telefone(s)", "p:contains('Telefone')")
	email := extractData(doc, "E-mail", "p:contains('E-mail')")

	
	fmt.Printf("CNPJ: %s\n", cnpjInfo)
	fmt.Printf("Razão Social: %s\n", razaoSocial)
	fmt.Printf("Nome Fantasia: %s\n", nomeFantasia)
	fmt.Printf("Data de Abertura: %s\n", dataAbertura)
	fmt.Printf("Telefone: %s\n", telefone)
	fmt.Printf("Email: %s\n", email)
}


func extractData(doc *goquery.Document, label string, selector string) string {
	var data string
	doc.Find(selector).Each(func(i int, s *goquery.Selection) {
		if s.Text() != "" && strings.Contains(strings.ToLower(label), strings.ToLower(s.Text())) {
			data = s.Text()
		}
	})
	return data
}
