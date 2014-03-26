package memory

import (
	"github.com/Clever/leakybucket"
	"testing"
)

func TestCreate(t *testing.T) {
	leakybucket.CreateTest(New(), "Memory")(t)
}

func TestThreadSafeAdd(t *testing.T) {
	leakybucket.ThreadSafeAddTest(New(), "Memory")(t)
}
