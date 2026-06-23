// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hotrowcontention

import (
	"os"
	"testing"

	"github.com/lesovsky/noisia/internal/dbtest"
)

func TestMain(m *testing.M) { os.Exit(dbtest.RunMain(m)) }
