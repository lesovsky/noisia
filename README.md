# Noisia

**Harmful workload generator for PostgreSQL.**

---

#### Supported workloads:
- `idle transactions` - transactions that do nothing during its lifetime.
- `rollbacks` - transactions performed some work but rolled back in the end.
- `waiting transactions` - transaction locked by other transactions and thus waiting.
- `deadlocks` - simultaneous transactions where each hold locks that the other transactions wants.
- `temporary files` - queries that produce on-disk temporary files due to lack of `work_mem`.
- `terminate backends` - terminate random backends (or queries) using `pg_terminate_backend()`, `pg_cancel_backend()`.
- `failed connections` - exhaust connections pool (other clients can't connect to Postgres) 
- ...see built-in help for more runtime options.

#### Disclaimer

ATTENTION: USE ONLY FOR TESTING PURPOSES, DO NOT EXECUTE NOISIA AGAINST PRODUCTION, RECKLESS USAGE WILL CAUSE PROBLEMS.

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
```
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

#### TODO/Ideas:
If you have any ideas, feel free to open a [discussion](https://github.com/lesovsky/noisia/discussions).

[ ] sequential scans

#### License
BSD-3. See [LICENSE](LICENSE) for more details.
