# Noisia

**Harmful workload generator for PostgreSQL.**

---

#### Supported workloads:
- `idle transactions` - active transactions on hot-write tables that do nothing during its lifetime.
- `rollbacks` - fake invalid queries that generate errors and increase rollbacks counter.
- `waiting transactions` - transactions that locks hot-write tables and then idle, that lead to stuck other transactions.
- `deadlocks` - simultaneous transactions where each hold locks that the other transactions wants.
- `temporary files` - queries that produce on-disk temporary files due to lack of `work_mem`.
- `terminate backends` - terminate random backends (or queries) using `pg_terminate_backend()`, `pg_cancel_backend()`.
- `failed connections` - exhaust all available connections (other clients unable to connect to Postgres).
- `fork connections` - execute single, short query in a dedicated connection (lead to excessive forking of Postgres backends).
- ...see built-in help for more runtime options.

#### Disclaimer

ATTENTION: USE ONLY FOR TESTING PURPOSES, DO NOT EXECUTE NOISIA WITHOUT COMPLETE UNDERSTANDING WHAT YOU REALLY DO, RECKLESS USAGE WILL CAUSE PROBLEMS.

DISCLAIMER: THIS SOFTWARE PROVIDED AS-IS WITH NO CARES AND GUARANTEES RELATED TO YOUR DATABASES. USE AT YOUR OWN RISK.


#### Installation and usage
Check out [releases](https://github.com/lesovsky/noisia/releases) page.
 
#### Using Docker
```shell script
docker pull lesovsky/noisia:latest
docker run --rm -ti lesovsky/noisia:latest noisia --help
```

#### Using in your own code
You can import `noisia` and use necessary workloads in your code. Always use contexts to avoid infinite run. See tiny example below:

```go
package main

import (
	"context"
	"fmt"
	"github.com/lesovsky/noisia/waitxacts"
	"github.com/rs/zerolog"
	"log"
	"os"
	"time"
)

func main() {
	config := waitxacts.Config{
		Conninfo:    "host=127.0.0.1",
		Jobs:        2,
		LocktimeMin: 5*time.Second,
		LocktimeMax: 20*time.Second,
	}

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).Level(zerolog.InfoLevel).With().Timestamp().Logger()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	w, err := waitxacts.NewWorkload(config, logger)
	if err != nil {
		log.Panicln(err)
	}
	
	err = w.Run(ctx)
	if err != nil {
		fmt.Println(err)
	}
}
```

#### Workload impact

Running workloads could impact on already running workload produced by other applications. This impact might be expressed as performance degradation, transactions stuck, cancelled queries, disconnected clients, etc.

| Workload  | Impact? |
| :---         |     :---:      |
| deadlocks  | No  |
| failconns  | **Yes**: exhaust `max_connections` limit; this leads to other clients are unable to connect to Postgres |
| forkconns  | **Yes**: excessive creation of Postgres child processes; potentially might lead to `max_connections` exhaustion |
| idlexacts  | **Yes**: might lead to tables and indexes bloat |
| rollbacks  | No  |
| tempfiles  | **Yes**: might increase storage utilization and degrade storage performance  |
| terminate  | **Yes**: already established database connections could be terminated accidentally  |
| waitxacts  | **Yes**: locks heavy-write tables; this leads to blocking concurrently executed queries  |

#### Contribution
- PR's are welcome.
- Ideas could be proposed [here](https://github.com/lesovsky/noisia/discussions)
- About grammar issues or typos let me know [here](https://github.com/lesovsky/noisia/discussions/8).

#### License
BSD-3. See [LICENSE](LICENSE) for more details.
