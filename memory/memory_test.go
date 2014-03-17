package memory

import (
	"github.com/Clever/leakybucket"
	"testing"
)

func TestCreate(t *testing.T) {
	leakybucket.CreateTest(New(), "Memory")(t)
}
