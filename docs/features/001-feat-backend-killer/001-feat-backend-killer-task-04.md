---
status: done                       # planned -> in_progress -> done
depends_on: ["01", "02", "03"]     # ID задач-зависимостей (строки: ["01", "02"])
wave: 3                            # волна параллельного выполнения
skills: [pre-deploy-qa]            # МАССИВ скиллов для загрузки
verify: bash                       # инструмент верификации (опционально: curl, bash, user)
reviewers: []                      # явно указать. Пусто = fallback на defaults
teammate_name:                     # имя агента-исполнителя (опционально; если не задано — генерируется по описанию задачи)
---

# Task 04: Pre-deploy QA

## Required Skills

Перед выполнением задачи загрузи:
- `/skill:pre-deploy-qa` — [skills/pre-deploy-qa/SKILL.md](~/.claude/skills/pre-deploy-qa/SKILL.md)

## Description

Финальная волна — приёмочное тестирование фичи `backend-killer` после того как все три
предыдущие задачи завершены (пакет `backendkiller`, проводка в CLI, README). Это QA-задача:
исполнитель не пишет продуктовый код, а прогоняет lint + полный набор тестов, собирает бинарь
и сверяет результат с критериями приёмки из user-spec и tech-spec.

**Деплоя нет.** Релизы noisia tag-driven (релиз создаётся пушем git-тега, см.
`deployment.md`), что выходит за рамки этой фичи. Задача проверяет готовность к коммиту/мержу,
а не выкатывает релиз.

Принципиальные точки проверки для этой разрушительной нагрузки: реальный OOM в CI **не**
запускается — тесты проверяют только механику (монотонный рост счётчика + чистый стоп по
context), `Conninfo` нигде не логируется (CWE-532), на дедикейтед-соединении выставлен
`application_name=noisia`, и присутствуют все 5 CLI-флагов с дефолтами/envars.

## What to do

1. Прогнать `make lint` — ожидается 0 issues по всем пакетам, включая новый `backendkiller`.
2. Прогнать полный набор тестов сериально: `make test` (= `go test -race -timeout 300s -p 1 ./...`).
   Убедиться, что:
   - тесты пакета `backendkiller` зелёные (config table-тест + механический тест: монотонный
     рост счётчика prepared statements за N итераций + чистая остановка по таймауту/отмене context);
   - нет регрессий в существующих пакетах (`rollbacks`, `tempfiles`, `idlexacts`, `db` и т.д.).
3. Собрать бинарь: `make build` (или `go build ./...`) — сборка проходит, нагрузка
   зарегистрирована в CLI.
4. Проверить `noisia --help`: присутствуют все 5 флагов backend-killer —
   `--backend-killer`, `--backend-killer.rate`, `--backend-killer.plan-size`,
   `--backend-killer.show-memory`, `--backend-killer.report-interval` — у каждого виден дефолт
   и envar (`NOISIA_...`). Флага `--jobs` для этой нагрузки нет.
5. Сверить критерии приёмки из user-spec и tech-spec (см. чек-лист в Acceptance Criteria ниже),
   опираясь на код, тесты и вывод `--help`/`README`. Где проверка автоматизирована — сослаться
   на прошедший тест; где статическая (логирование Conninfo, application_name, README) — открыть
   соответствующий файл и подтвердить.
6. Зафиксировать вердикт QA (PASS/FAIL) с перечислением проверенного и любых найденных
   расхождений. Найденное несоответствие AC — это FAIL: вернуть на доработку, не «замазывать».

## Acceptance Criteria

Чек-лист QA (сверка с user-spec «Критерии приёмки» и tech-spec «Acceptance Criteria»):

- [ ] `make lint` — 0 issues по всем пакетам.
- [ ] `make test` (`go test -race -timeout 300s -p 1 ./...`) — зелёный, без регрессий.
- [ ] Тесты пакета `backendkiller` зелёные: config table-тест + механический тест (монотонный
      рост счётчика + чистый стоп по context; реальный OOM не запускается).
- [ ] `make build` / `go build ./...` проходит; нагрузка зарегистрирована в CLI.
- [ ] `noisia --help` показывает все 5 флагов: `--backend-killer`, `--backend-killer.rate`
      (0=без лимита), `--backend-killer.plan-size` (int с дефолтом-константой),
      `--backend-killer.show-memory`, `--backend-killer.report-interval` (1s) — с дефолтами и envars.
- [ ] `--jobs` для backend-killer не применяется (нагрузка однопоточная).
- [ ] Нагрузка использует одно выделенное соединение (`db.Connect`), не пул; нет предзасева.
- [ ] Уникальные литеральные серверные `PREPARE` через `Conn.Exec`; внутренний счётчик растёт
      монотонно; «тяжесть» плана зависит от `plan-size`.
- [ ] `Rate=0` ⇒ unlimited (`rate.Inf`); `Rate>0` ⇒ троттлинг; `validate()` enforce
      `Rate>=0`, `PlanSize>=1`, `ReportInterval>0`.
- [ ] Панель эскалации пишется раз в `report-interval` через `logger.Infof`; опроса состояния
      сервера нет; `--show-memory` добавляет память своего backend'а (PG14+, на PG<14 — мягкая
      деградация).
- [ ] Обработка ошибок: connect-failure на старте ⇒ init error; mid-run `Exec` error под живым
      context с counter>0 ⇒ кульминационная строка + возврат `nil`; first-`Exec` error с
      counter==0 ⇒ возвращается как init error; non-connection query error ⇒ `Warnf` + continue.
- [ ] Чистая остановка по отмене/таймауту context (возврат `nil`).
- [ ] `Conninfo` нигде не логируется, включая обёрнутые connect/`Exec` ошибки (фиксированное
      сообщение + санитизированная причина) — подтверждено просмотром кода.
- [ ] `application_name=noisia` выставлен на дедикейтед-соединении (атрибутируется в
      `pg_stat_activity`) — подтверждено просмотром кода.
- [ ] README обновлён: bullet-список нагрузок + строка в таблице impact (impact = Yes).
- [ ] Итоговый вердикт QA зафиксирован (PASS/FAIL) с перечнем проверенного и расхождений.

## Verification Steps

- `make lint` → вывод без ошибок (0 issues).
- `make test` → все пакеты PASS; в выводе видны прошедшие тесты `backendkiller` (config +
  механика).
- `make build` (или `go build ./...`) → бинарь собран без ошибок.
- `./noisia --help` (или путь к собранному бинарю) → в выводе присутствуют 5 флагов
  backend-killer с дефолтами и envars; `--jobs` среди них нет.
- `grep -n backend-killer README.md` → видны bullet-список и строка таблицы impact (Yes).
- Открыть `backendkiller/backendkiller.go` → убедиться: нет интерполяции `Conninfo` в логах/ошибках;
  `application_name=noisia` выставляется на соединении.
- Сводный результат: все пункты Acceptance Criteria отмечены; вердикт QA = PASS (иначе вернуть
  на доработку с перечнем FAIL-пунктов).

## Details

**Files:** код менять не нужно — это приёмочная проверка. Чтение для сверки:
`backendkiller/backendkiller.go` (Conninfo-логирование, `application_name`, механика),
`cmd/main.go` + `cmd/app.go` (5 флагов, дефолты/envars, отсутствие `--jobs`), `README.md`
(bullet + impact-таблица), тесты `backendkiller/*_test.go`.

**Dependencies:**
- Задачи 01 (пакет), 02 (CLI), 03 (README) должны быть завершены.
- Локально доступен Docker (testcontainers поднимает `postgres:15-alpine` через `dbtest.RunMain`).
- Цели Makefile: `make lint`, `make test`, `make build`.

**Edge cases:**
- Тесты сериализуются (`-p 1`) — это норма, не флак.
- Реальный OOM в CI **не** воспроизводится; механический тест не должен доводить инстанс до OOM.
- `--show-memory` на PG<14 контейнером не тестируется (известное ограничение; деградация
  проверяется ручным прогоном на стенде, вне рамок этой задачи).
- Ручной прогон на стенде с ограниченной памятью (реальный OOM → перезапуск инстанса →
  кульминационная строка) — зона ответственности пользователя, **не** часть агентской QA.

**Implementation hints:**
- Если `make lint` падает на форматировании/импортах — это FAIL задачи 01/02, вернуть на
  доработку, не править здесь.
- Проверку `Conninfo`-логирования и `application_name` делать статически (чтение кода): grep по
  `Conninfo`/`application_name` в `backendkiller/` плюс просмотр путей логирования ошибок
  (connect-failure, per-tick `Exec` error).
- Деплой не выполнять: релизы tag-driven, вне рамок фичи (см. `deployment.md`).

## Context Files

**Feature artifacts:**
- [001-feat-backend-killer.md](001-feat-backend-killer.md) — user-spec («Как проверить», «Критерии приёмки»)
- [001-feat-backend-killer-tech-spec.md](001-feat-backend-killer-tech-spec.md) — tech-spec (Task 4, Acceptance Criteria, Agent Verification Plan)
- [001-feat-backend-killer-decisions.md](001-feat-backend-killer-decisions.md) — decisions log

**Project knowledge:**
- [project.md](../../../.claude/skills/project-knowledge/references/project.md)
- [architecture.md](../../../.claude/skills/project-knowledge/references/architecture.md)
- [patterns.md](../../../.claude/skills/project-knowledge/references/patterns.md) — Testing & verification, git/security gates
- [deployment.md](../../../.claude/skills/project-knowledge/references/deployment.md) — tag-driven releases (подтверждает: деплой вне рамок)

**Code files (для сверки, не для правки):**
- [backendkiller/backendkiller.go](../../../backendkiller/backendkiller.go) — Conninfo-логирование, application_name, механика
- [cmd/main.go](../../../cmd/main.go) — флаги, дефолты, envars
- [cmd/app.go](../../../cmd/app.go) — проводка нагрузки
- [README.md](../../../README.md) — bullet-список + таблица impact

## Reviewers

- Нет ревьюеров (QA-задача).

## Post-completion

- [ ] Записать краткий отчёт в [001-feat-backend-killer-decisions.md](001-feat-backend-killer-decisions.md) (Summary: 1-3 предложения, вердикт QA PASS/FAIL, перечень проверенного и расхождений)
- [ ] Если отклонились от спека — описать отклонение и причину
- [ ] Обновить user-spec/tech-spec если что-то изменилось
