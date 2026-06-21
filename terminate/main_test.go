package terminate

import (
	"os"
	"testing"

	"github.com/lesovsky/noisia/internal/dbtest"
)

func TestMain(m *testing.M) { os.Exit(dbtest.RunMain(m)) }
