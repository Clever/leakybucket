package memory

import (
	"github.com/Clever/leakybucket"
	"testing"
)

func TestCreate(t *testing.T) {
	leakybucket.CreateTest(New())(t)
}

func TestAdd(t *testing.T) {
	leakybucket.AddTest(New())(t)
}

func TestThreadSafeAdd(t *testing.T) {
	leakybucket.ThreadSafeAddTest(New())(t)
}
