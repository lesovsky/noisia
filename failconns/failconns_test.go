package failconns

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestWorkload_Run(t *testing.T) {
	config := &Config{
		PostgresConninfo: "host=127.0.0.1",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w := NewWorkload(config)
	err := w.Run(ctx)
	assert.Nil(t, err)
}

func TestExample(t *testing.T) {
	s := make([]*int, 0, 10)
	fmt.Println("len: ", len(s))
	fmt.Println("cap: ", cap(s))

	a, b, c := 1, 2, 3
	var x, y, z = &a, &b, &c

	s = append(s, x, y, z)
	fmt.Println("len: ", len(s))
	fmt.Println("cap: ", cap(s))
}
