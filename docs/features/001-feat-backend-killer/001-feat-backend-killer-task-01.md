---
status: planned                    # planned -> in_progress -> done
depends_on: []                     # ID задач-зависимостей (строки: ["01", "02"])
wave: 1                            # волна параллельного выполнения
skills: [code-writing]             # МАССИВ скиллов для загрузки
verify: bash — `go test -race -p 1 ./backendkiller/`   # инструмент верификации
reviewers: [dev-code-reviewer, dev-security-auditor, dev-test-reviewer]  # явно указать. Пусто = fallback на defaults
teammate_name:                     # имя агента-исполнителя (опционально; если не задано — генерируется по описанию задачи)
---

# Task 01: Implement `backendkiller` workload package

## Required Skills

Перед выполнением задачи загрузи:
- `/skill:code-writing` — [skills/code-writing/SKILL.md](~/.claude/skills/code-writing/SKILL.md)

## Description

Создаём новый noisia-пакет нагрузки `backendkiller`, повторяющий каноническую форму нагрузки
(`Config` + `validate()` + `NewWorkload` + `Run(ctx)`), как в `rollbacks`/`tempfiles`/`idlexacts`.

Поведение: одна выделенная сессия (`db.Connect`, **не пул**) в однопоточном rate-limited цикле
непрерывно шлёт уникальные литеральные серверные `PREPARE noisia_bk_<i> AS SELECT 0 AS c0, 1 AS c1, …`
через `Conn.Exec`. План-кэш backend'а растёт без границ → RSS лезет вверх → OOM killer убивает
backend → postmaster перезапускает инстанс. Нагрузка ведёт самоотчёт (in-process счётчик созданных
statements), печатает панель эскалации раз в `report-interval`, а при обрыве собственного соединения
(после хотя бы одного успешного `PREPARE`) печатает кульминационную строку и завершается `nil`.

Это **только пакет `backendkiller`** (.go + два тест-файла). CLI-обвязка (`cmd/`) и README — отдельные
задачи (Wave 2 / Wave 3), здесь их НЕ трогаем.

Задача должна честно соблюсти все 9 Decisions из tech-spec — особенно: `application_name=noisia` на
соединении (D8), `Conninfo` нигде не логируется включая обёрнутые ошибки (D9), отдельная goroutine
report-ticker читает только atomic-счётчик и не трогает `Conn` (D6), и first-`Exec` guard, который
возвращает init-ошибку при counter==0 (D5).

## What to do

1. Создать `backendkiller/backendkiller.go`:
   - 3-строчный BSD-заголовок (как в `idlexacts/idlexacts.go:1-3`) + package-doc-комментарий,
     описывающий механизм (утечка plan cache → OOM → рестарт инстанса).
   - `type Config struct { Conninfo string; Rate float64; PlanSize int; ShowMemory bool; ReportInterval time.Duration }`
     ровно как в Data Models tech-spec (`Conninfo` первым полем, секрет — не логировать).
   - `func (c Config) validate() error` (value receiver, unexported, первое нарушенное правило через
     `fmt.Errorf`): `Conninfo` непустой; `Rate >= 0` (ВНИМАНИЕ — допускаем `0`, не копировать
     `Rate <= 0` из rollbacks); `PlanSize >= 1`; `ReportInterval > 0`.
   - `type workload struct { config Config; logger log.Logger }` (форма rollbacks; conn открывается
     внутри `Run`, поля conn в структуре нет).
   - `func NewWorkload(config Config, logger log.Logger) (noisia.Workload, error)` — сначала
     `config.validate()`, при ошибке `nil, err`, иначе `&workload{config, logger}, nil`.
   - `func (w *workload) Run(ctx context.Context) error` — открыть одно соединение через
     `db.Connect(ctx, w.config.Conninfo)`; ошибку connect вернуть как init-ошибку (D9: фиксированное
     сообщение + sanitized cause, без `Conninfo`); `defer conn.Close()`; обеспечить
     `application_name=noisia` на соединении (D8); запустить report-ticker в ОТДЕЛЬНОЙ goroutine
     (читает только atomic-счётчик и atomic-память, в `Conn` не ходит); крутить главный loop.
   - Heavy-`PREPARE` builder (единственный по-настоящему новый кусок): функция, строящая по
     `(i int, planSize int)` строку `PREPARE noisia_bk_<i> AS SELECT 0 AS c0, 1 AS c1, …, (N-1) AS c{N-1}`
     через `strings.Builder`/`fmt` (только `%d`-форматирование валидированных int — без bind-args, без
     внешнего ввода). Выделить как отдельную unit-тестируемую функцию.
   - Главный loop (зеркало `rollbacks.startLoop`, `rollbacks/rollbacks.go:111-141`): `rate.NewLimiter`
     с `rate.Inf` при `Rate==0`, иначе `rate.Limit(Rate)` (D2); на `Allow()` — собрать heavy-`PREPARE`
     с монотонным счётчиком, `conn.Exec`, инкремент atomic-счётчика (D3); единственный путь остановки —
     `select { case <-ctx.Done(): return nil; default: }`.
   - Climax-детекция (D5): `Exec`-ошибка при `ctx.Err()==nil` И счётчик>0 → залогировать
     `connection lost after T, N statements issued — target likely OOM-restarted` и вернуть `nil`;
     первая `Exec`-ошибка при счётчике==0 → вернуть init-ошибку (без `Conninfo`); per-tick
     не-connection ошибка запроса → `Warnf` (без `Conninfo`) + continue.
   - Опциональный `--show-memory` (D6): когда `ShowMemory==true`, ГЛАВНАЯ goroutine (владелец `Conn`),
     gating по `time.Since(lastMemoryRead) >= ReportInterval` после `Exec`, выполняет
     `SELECT sum(used_bytes) FROM pg_backend_memory_contexts`, публикует значение в atomic для тикера и
     сбрасывает timestamp. Ошибка чтения памяти — per-tick recoverable (`Warnf` один раз, пропустить
     поле, продолжить); на PG<14 (нет relation) — мягкая деградация (warn один раз, дропнуть поле,
     счётчик оставить). Эта ошибка НЕ триггерит климакс.
   - Панель эскалации (D6): report-ticker раз в `ReportInterval` через `logger.Infof` печатает
     `backend-killer: prepared stmts=N rate=R/s elapsed=T` (rate — из дельты atomic-счётчика; `+ backend mem≈…`
     при `--show-memory`). Тикер читает ТОЛЬКО atomic'и, к `Conn` не обращается (pgx `Conn` не
     concurrency-safe).

2. Создать `backendkiller/main_test.go` — ровно одна строка тела
   (`func TestMain(m *testing.M) { os.Exit(dbtest.RunMain(m)) }`), `package backendkiller`, импорты
   `os`, `testing`, `github.com/lesovsky/noisia/internal/dbtest` (см. `rollbacks/main_test.go`).

3. Создать `backendkiller/backendkiller_test.go` с BSD-заголовком и:
   - `TestConfig_validate` (table test): валидные (`Rate=0`, `Rate>0`, `PlanSize>=1`, `ReportInterval>0`,
     непустой `Conninfo`) и невалидные (`PlanSize<1`, `ReportInterval<=0`, пустой `Conninfo`, `Rate<0`).
   - Unit-тест heavy-`PREPARE` builder: при заданном `PlanSize` строка well-formed, содержит ровно
     `PlanSize` выражений в target-list, имя уникально для разных `i`, содержит только числа/имя.
   - `TestWorkload_Run` (testcontainers через `db.TestConninfo`): короткий `context.WithTimeout` (~1s)
     + throttled `Rate`; in-process счётчик растёт монотонно за N итераций; `Run` возвращает `nil` по
     отмене/таймауту ctx. **Реальный OOM не воспроизводим.** Чтобы проверить монотонный рост, выделить
     loop-helper, возвращающий финальный счётчик (как `Test_startLoop` в `rollbacks_test.go:70-81`,
     `assert.Positive`), либо снять два снимка atomic-счётчика.
   - Invalid-conninfo case: `Config{Conninfo: "database=noisia_invalid", …}` → `Run` (или `NewWorkload`)
     возвращает ошибку.

## TDD Anchor

Тесты пишем ДО реализации, запускаем (падают) → пишем код → проходят.

- `backendkiller/backendkiller_test.go::TestConfig_validate` — table-тест: валидно при `Rate=0`,
  `Rate>0`, `PlanSize>=1`, `ReportInterval>0`, непустом `Conninfo`; невалидно при `PlanSize<1`,
  `ReportInterval<=0`, пустом `Conninfo`, `Rate<0`.
- `backendkiller/backendkiller_test.go::Test_buildPrepare` (имя helper по факту) — heavy-`PREPARE`
  builder для `PlanSize=N` даёт строку с ровно N target-list выражениями, уникальным именем
  `noisia_bk_<i>` для разных `i`, без bind-аргументов.
- `backendkiller/backendkiller_test.go::TestWorkload_Run` — интеграция (testcontainers): счётчик
  созданных statements растёт монотонно за N итераций при throttled `Rate`; `Run` возвращает `nil`
  по таймауту ctx (без реального OOM); с `Conninfo="database=noisia_invalid"` → ошибка.

## Acceptance Criteria

- [ ] Пакет `backendkiller` реализует `Config`/`validate()`/`NewWorkload`/`Run(ctx)` по канонической форме.
- [ ] Используется одно соединение `db.Connect` (не пул); однопоточно; без предзасева.
- [ ] Уникальные литеральные серверные `PREPARE` через `Conn.Exec`; in-process счётчик растёт монотонно.
- [ ] `Rate=0` ⇒ unlimited (`rate.Inf`); `Rate>0` ⇒ throttled; `validate()` enforces `Rate>=0`,
      `PlanSize>=1`, `ReportInterval>0`, непустой `Conninfo`.
- [ ] Панель эскалации `backend-killer: prepared stmts=N rate=R/s elapsed=T` пишется раз в
      `report-interval` через `logger.Infof`; при `--show-memory` добавляется память своего backend'а
      (PG14+, мягкая деградация ниже).
- [ ] Connect failure на старте ⇒ init-ошибка; mid-run `Exec`-ошибка при живом ctx и counter>0 ⇒
      кульминационная строка + return `nil`; первая `Exec`-ошибка при counter==0 ⇒ init-ошибка;
      non-connection query error ⇒ `Warnf` + continue.
- [ ] Report-ticker в своей goroutine читает только atomic-счётчик; `--show-memory` запрос делает
      loop-goroutine (владелец `Conn`), никогда конкурентно на общем `Conn`.
- [ ] `application_name=noisia` установлен на выделенном соединении (атрибутируется в `pg_stat_activity`).
- [ ] Ни одна лог-строка не интерполирует `Conninfo`, включая обёрнутые connect/`Exec` ошибки
      (фиксированное сообщение + sanitized cause).
- [ ] Heavy-`PREPARE` builder потребляет только валидированные int (`PlanSize>=1`, `%d`-форматирование
      имени/выражений) — покрыто unit-тестом (нет поверхности инъекции).
- [ ] Чистая остановка по отмене/таймауту ctx (возврат `nil`).
- [ ] `Config.validate()` покрыт table-тестом; интеграционный тест проверяет монотонный рост счётчика +
      чистый стоп (без реального OOM); invalid-conninfo → ошибка.
- [ ] BSD 3-строчный заголовок в каждом `.go`-файле; `go test -race -p 1 ./backendkiller/` зелёный.

## Context Files

**Feature artifacts:**
- [001-feat-backend-killer.md](docs/features/001-feat-backend-killer/001-feat-backend-killer.md) — user-spec
- [001-feat-backend-killer-tech-spec.md](docs/features/001-feat-backend-killer/001-feat-backend-killer-tech-spec.md) — tech-spec (Solution, Architecture, ВСЕ 9 Decisions, Data Models, Testing Strategy, AC, "Task 1")
- [001-feat-backend-killer-code-research.md](docs/features/001-feat-backend-killer/001-feat-backend-killer-code-research.md) — точные сигнатуры, loop-идиома, builder, тест-паттерны
- [001-feat-backend-killer-decisions.md](docs/features/001-feat-backend-killer/001-feat-backend-killer-decisions.md) — decisions log

**Project knowledge:**
- [project.md](.claude/skills/project-knowledge/references/project.md)
- [architecture.md](.claude/skills/project-knowledge/references/architecture.md)
- [patterns.md](.claude/skills/project-knowledge/references/patterns.md) — workload shape, Testing & Verification (`-p 1`, testcontainers, `db.TestConninfo`), "Conninfo — секрет"

**Code files (modify):**
- [backendkiller/backendkiller.go](backendkiller/backendkiller.go) — новый файл (пакет нагрузки)
- [backendkiller/backendkiller_test.go](backendkiller/backendkiller_test.go) — новый файл (config table test + builder unit test + integration)
- [backendkiller/main_test.go](backendkiller/main_test.go) — новый файл (одна строка `TestMain` → `dbtest.RunMain`)

**Code files (read for context):**
- [rollbacks/rollbacks.go](rollbacks/rollbacks.go) — каноническая single-`Conn` форма; `startLoop` (rollbacks.go:111-141); `runWorker` (rollbacks.go:93-108); `validate` (rollbacks.go:43-53)
- [tempfiles/tempfiles.go](tempfiles/tempfiles.go) — `ctx.Err()==nil` guard (tempfiles.go:141); `Query`/`Scan` pattern в `countTempBytes` (tempfiles.go:178-201)
- [rollbacks/rollbacks_test.go](rollbacks/rollbacks_test.go) — `TestConfig_validate` (rollbacks_test.go:12-29), `TestWorkload_Run` (51-61), `Test_startLoop` (70-81)
- [db/db.go](db/db.go) — `Conn` interface (db.go:24-29): `Exec(ctx,sql,args)->(int64,string,error)`, `Query`, `Close() error`
- [db/postgres.go](db/postgres.go) — `Connect` (postgres.go:113-122); `Conn.Exec` (136-143); `Conn.Close` (150-152); `application_name=noisia` ставится в `NewPostgresDB` (23), но НЕ в `Connect`
- [noisia.go](noisia.go) — `Workload interface { Run(context.Context) error }`
- [log/log.go](log/log.go) — `Logger`: `Info/Infof/Warn/Warnf/Error/Errorf` (нет структурных полей/Debug)

## Verification Steps

- Запустить `go test -race -p 1 ./backendkiller/` — зелёный (config table test + builder unit test +
  integration: монотонный рост счётчика + чистый стоп по ctx + invalid-conninfo).
- Запустить `gofmt -l backendkiller/` — пусто (форматирование чистое).
- Грепом убедиться, что `Conninfo` не попадает в лог: ни одна `logger.*`/возвращаемая ошибка не
  интерполирует `c.Conninfo` или сырую ошибку connect/Exec, способную нести DSN.
- Убедиться, что пакет НЕ импортирует `targeting` и НЕ использует пул (`db.NewPostgresDB`); только
  `db.Connect`.

## Details

**Files:**
- `backendkiller/backendkiller.go` — новый. Package-doc + Config + `validate()` + `workload` +
  `NewWorkload` + `Run` + heavy-`PREPARE` builder + report-ticker + опциональный memory-read + climax.
- `backendkiller/main_test.go` — новый. Ровно одна строка тела (см. `rollbacks/main_test.go`).
- `backendkiller/backendkiller_test.go` — новый. Config table test + builder unit test +
  `TestWorkload_Run` integration + invalid-conninfo.

**Dependencies:**
- Новых пакетов нет. Существующие: `github.com/lesovsky/noisia` (`Workload`),
  `github.com/lesovsky/noisia/db` (`db.Connect`, `Conn`), `github.com/lesovsky/noisia/log` (`Logger`),
  `golang.org/x/time/rate` (`NewLimiter`, `rate.Inf`, `rate.Limit`, `Allow`). Для тестов:
  `github.com/lesovsky/noisia/internal/dbtest`, `db.TestConninfo`, `github.com/stretchr/testify/assert`.
- Задача атомарна, `depends_on: []`. Wave 2 (CLI) и Wave 3 (README) зависят от этого пакета.

**Edge cases:**
- `Rate=0` → `rate.Inf` (D2): тот же `Allow()`+`select{ctx.Done()}` loop, без отдельной busy-loop ветки.
- Первая `Exec`-ошибка (counter==0) → init-ошибка, НЕ климакс (D5: не маскировать баг builder'а).
- Mid-run `Exec`-ошибка при `ctx.Err()==nil` и counter>0 → климакс (return `nil`).
- Per-tick non-connection query error → `Warnf` + continue.
- ctx отменён до OOM (типичный CI/тест случай) → чистый `nil`.
- `--show-memory` на PG<14 → relation отсутствует → warn один раз, дропнуть поле памяти, счётчик
  оставить; не климакс.
- Сбой чтения памяти около OOM → per-tick recoverable, не климакс.
- Уникальность имён `PREPARE` на миллионах statements → монотонный int-счётчик `noisia_bk_<i>`.

**Implementation hints (НЕ псевдокод):**
- Каноническую форму Config/validate/workload/NewWorkload/Run бери из `rollbacks/rollbacks.go:32-69` и
  `:93-108`. ВНИМАНИЕ к диверсии: НЕ копируй `Rate <= 0` reject — `Rate>=0` валидно (D2).
- Loop — точная форма `rollbacks.startLoop` (`rollbacks/rollbacks.go:111-141`): `rate.NewLimiter(lim, 1)`
  с burst=1, busy-`Allow()` poll (НЕ `Wait(ctx)` — его в репо никто не использует),
  `select { case <-ctx.Done(): return nil; default: }`.
- `rate.Inf` = `Limit(math.MaxFloat64)`; limiter с `Inf` всегда `Allow()==true`. Идиома:
  `lim := rate.Limit(r); if r <= 0 { lim = rate.Inf }; limiter := rate.NewLimiter(lim, 1)`.
- `Conn.Exec` возвращает `(int64, string, error)` (`db/postgres.go:136-143`); heavy-`PREPARE` — это
  чистый DDL-style `Exec` БЕЗ args (как `createTempTable`, `rollbacks/rollbacks.go:148`). Имя стейтмента
  и ширина target-list — в тексте SQL, не в bind-аргументах (`PREPARE` сам определяет параметры).
- Heavy-`PREPARE` builder: `strings.Builder` + `fmt.Fprintf(&b, "PREPARE noisia_bk_%d AS SELECT ", i)`,
  затем цикл по `j := 0..planSize-1` с `%d AS c%d` через запятую; без `FROM`, без таблицы. Только
  валидированные int — нет injection surface (покрыть unit-тестом на число выражений и уникальность имени).
- `ctx.Err()==nil` guard перед логированием — идиома `tempfiles/tempfiles.go:141`, чтобы не шуметь на
  отмене ctx.
- `--show-memory`: `SELECT sum(used_bytes) FROM pg_backend_memory_contexts` через `conn.Query` +
  `rows.Next()`/`rows.Scan(&x)`/`rows.Close()` (паттерн `tempfiles/countTempBytes`, tempfiles.go:188-198).
  Делает ТОЛЬКО loop-goroutine (владелец `Conn`), gating по `time.Since(lastMemoryRead) >= ReportInterval`;
  публиковать в atomic для тикера. pgx `Conn` НЕ concurrency-safe — тикер `Conn` не трогает.
- Самоотчёт: счётчик — `sync/atomic` (`atomic.Int64` или `atomic.AddInt64`); тикер читает atomic, считает
  rate из дельты за `ReportInterval`, печатает `logger.Infof("backend-killer: prepared stmts=%d rate=%.0f/s elapsed=%s", …)`.
- D8 (`application_name=noisia`): `Connect` его НЕ ставит (в отличие от `NewPostgresDB`,
  `db/postgres.go:23`). Поставить локально: либо `SET application_name = 'noisia'` через `conn.Exec` сразу
  после connect, либо дописать в conninfo если отсутствует. `db.Connect` НЕ модифицировать (out of scope).
- D9 (`Conninfo` не логировать): connect-failure и per-tick `Exec`-ошибка — фиксированное сообщение +
  sanitized cause; никогда не класть `c.Conninfo` или сырую ошибку, способную нести DSN, в лог/возвращаемую
  ошибку. `db.Connect` оборачивает `pgx.Connect` без скраббинга — обернуть/санитизировать на своей стороне.
- `defer conn.Close()` для чистоты (`failconns`/`forkconns`/`tempfiles` закрывают; `rollbacks` — нет;
  здесь закрываем). `Conn.Close() error` (`db/postgres.go:150-152`).
- НЕ импортировать `targeting` (нагрузка самогенерируется). НЕ использовать пул `db.NewPostgresDB`.
- Тесты сериализуются `-p 1` (`Makefile`); держи короткие детерминированные тайминги (~1s `WithTimeout`).
  `db.TestConninfo` заполняется `dbtest.RunMain` из `TestMain`.

## Reviewers

- **dev-code-reviewer** → `docs/features/001-feat-backend-killer/001-feat-backend-killer-task-01-dev-code-reviewer-review.json`
- **dev-security-auditor** → `docs/features/001-feat-backend-killer/001-feat-backend-killer-task-01-dev-security-auditor-review.json`
- **dev-test-reviewer** → `docs/features/001-feat-backend-killer/001-feat-backend-killer-task-01-dev-test-reviewer-review.json`

## Post-completion

- [ ] Записать краткий отчёт в [001-feat-backend-killer-decisions.md](docs/features/001-feat-backend-killer/001-feat-backend-killer-decisions.md) (Summary: 1-3 предложения, ревью со ссылками на JSON, без таблиц файндингов и дампов)
- [ ] Если отклонились от спека — описать отклонение и причину
- [ ] Обновить user-spec/tech-spec если что-то изменилось
