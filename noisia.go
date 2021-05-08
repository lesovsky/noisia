package noisia

import (
	"context"
)

type Workload interface {
	Run(context.Context) error
}
