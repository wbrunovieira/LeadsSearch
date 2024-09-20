
package googleplaces

import (
	"testing"
)

func TestNewService(t *testing.T) {
    apiKey := "test-key"
    service := NewService(apiKey)
    if service.APIKey != apiKey {
        t.Errorf("expected APIKey %v, got %v", apiKey, service.APIKey)
    }
}
