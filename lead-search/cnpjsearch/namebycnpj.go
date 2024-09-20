package cnpjsearch

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)


func FetchData(companyName string,cityName string) {
	
	searchQuery := strings.ReplaceAll(companyName, " ", "%20")
	url := fmt.Sprintf("https://cnpj.biz/procura/%s", searchQuery)

	
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

	
	doc.Find(".flex.items-center.text-sm.text-gray-500").Each(func(index int, element *goquery.Selection) {
		cnpjData := strings.TrimSpace(element.Text())
		if strings.Contains(cnpjData, "/") {
			
			cnpjParts := strings.Split(cnpjData, "/")
			if len(cnpjParts) >= 2 {
				cnpjCity := strings.TrimSpace(cnpjParts[1]) 

			
				if strings.Contains(strings.ToLower(cnpjCity), strings.ToLower(cityName)) {
					cnpj := strings.TrimSpace(cnpjParts[0])
					fmt.Printf("CNPJ encontrado: %s\n", cnpj)
					fmt.Printf("Cidade: %s\n", cnpjCity)             
					FetchCompanyDetails(cnpj)
				}
			}
		}
	})
	
	SleepRandom()
}


func SleepRandom() {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	time.Sleep(time.Duration(2+rng.Intn(2)) * time.Second)
}

