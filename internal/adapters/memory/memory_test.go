package memory

import (
	"testing"

	"github.com/laurentpoirierfr/go-api-skeleton/internal/app"
	"github.com/laurentpoirierfr/go-api-skeleton/internal/app/apptest"
)

func TestContract(t *testing.T) {
	apptest.Run(t, func(*testing.T) app.UnitOfWork { return NewStore() })
}
