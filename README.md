# Noisia

**Harmful workload generator for PostgreSQL.**

---

#### Supported workloads:
- `idle transactions` - active transactions on hot-write tables that do nothing during their lifetime.
- `rollbacks` - fake invalid queries that generate errors and increase rollbacks counter.
- `waiting transactions` - transactions that lock hot-write tables and then idle, that lead to stuck other transactions.
- `deadlocks` - simultaneous transactions where each hold locks that the other transactions want.
- `temporary files` - queries that produce on-disk temporary files due to lack of `work_mem`.
- `terminate backends` - terminate random backends (or queries) using `pg_terminate_backend()`, `pg_cancel_backend()`.
- `failed connections` - exhaust all available connections (other clients unable to connect to Postgres) 
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
import (
  "context"
  "fmt"
  "github.com/lesovsky/noisia/waitxacts"
  "time"
)

func main() {
  config := &waitxacts.Config{
    PostgresConninfo:    "host=127.0.0.1",
    Jobs:                 2,
  }

  ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
  defer cancel()

  w := waitxacts.NewWorkload(config)
  if err := w.Run(ctx); err != nil {
    fmt.Println(err)
  }
}
```

#### Workload impact

Running workloads could impact on already running workload produced by other applications. This impact might be expressed as performance degradation, transactions stuck, canceled queries, disconnected clients, etc.

| Workload  | Impact? |
| :---         |     :---:      |
| deadlocks  | No  |
| failconns  | **Yes**  |
| idlexacts  | **Yes**  |
| rollbacks  | No  |
| tempfiles  | No  |
| terminate  | **Yes**  |
| waitxacts  | **Yes**  |

#### Contribution
- PR's are welcome.
- Ideas could be proposed [here](https://github.com/lesovsky/noisia/discussions)
- About grammar issues or typos let me know [here](https://github.com/lesovsky/noisia/discussions/8).

#### License
BSD-3. See [LICENSE](LICENSE) for more details.
