---
status: planned                    # planned -> in_progress -> done
depends_on: []                     # ID задач-зависимостей (строки: ["01", "02"])
wave: 2                            # волна параллельного выполнения
skills: [documentation-writing]    # МАССИВ скиллов для загрузки
verify: bash                       # инструмент верификации (опционально: curl, bash, user)
reviewers: [dev-code-reviewer]     # явно указать. Пусто = fallback на defaults
teammate_name:                     # имя агента-исполнителя (опционально; если не задано — генерируется по описанию задачи)
---

# Task 03: Update README

## Required Skills

Перед выполнением задачи загрузи:
- `/skill:documentation-writing` — [skills/documentation-writing/SKILL.md](~/.claude/skills/documentation-writing/SKILL.md)

## Description

Задокументировать новую нагрузку `backend-killer` в `README.md`, чтобы она была видна наравне
с остальными восемью нагрузками: одна сессия раздувает память своего backend-процесса утечкой
prepared statements (рост plan cache), пока OOM killer не убьёт backend и postmaster не
перезапустит весь инстанс. README — единственный пользовательский документ проекта (CLI-утилита,
без отдельной документации), поэтому он должен описывать поведение нагрузки и её разрушительный
эффект.

Это чисто документационная задача: меняется только текст в `README.md`, код не трогаем. Нагрузка
уже реализована (Task 1) и подключена в CLI (Task 2) — здесь мы лишь приводим README в
соответствие с фактом её появления, как требует критерий приёмки tech-spec («README updated:
bullet list + impact table = Yes»).

## What to do

1. Добавить нагрузку в список поддерживаемых нагрузок (bullet-список в начале README) — одну
   строку в стиле существующих пунктов: краткое описание эффекта «одна сессия утечкой prepared
   statements (рост plan cache) доводит backend до OOM и перезапуска инстанса». Вставить перед
   завершающим пунктом `- ...see built-in help for more runtime options.`, сохранив стиль и
   формат соседних строк.
2. Добавить строку в таблицу «Workload impact» с impact = **Yes**: одна сессия может растить RSS
   backend'а до OOM-kill и полного перезапуска инстанса. Соблюсти алфавитный порядок строк
   таблицы и формат `| name | impact |` как у соседних строк (имя пакета, как
   `forkconns`/`idlexacts`, не флаг с дефисом).
3. Отразить операционную оговорку: очень большой `--backend-killer.plan-size` делает каждый
   `PREPARE` тяжёлым/медленным — встроить это в формулировку impact-строки или bullet-пункта так,
   чтобы оператор понимал цену повышения `plan-size`. Не добавлять отдельную секцию ради одной
   фразы.
4. Не дублировать дисклеймер и не менять прочие секции README (Disclaimer уже покрывает
   разрушительность; код-пример с `waitxacts` не трогаем).

## Acceptance Criteria

- [ ] В bullet-списке поддерживаемых нагрузок есть пункт про `backend-killer`, описывающий OOM
      через утечку prepared statements (рост plan cache) и перезапуск инстанса; стиль/формат
      совпадает с соседними пунктами.
- [ ] В таблице «Workload impact» есть строка с импактом **Yes**: одна сессия растит RSS
      backend'а до OOM-kill и полного перезапуска инстанса.
- [ ] Отражена оговорка про очень большой `--backend-killer.plan-size` (тяжёлый/медленный
      `PREPARE`).
- [ ] Алфавитный порядок строк impact-таблицы сохранён; формат строк `| name | impact |` не нарушен.
- [ ] Прочие секции README не изменены; код-пример и дисклеймер не тронуты.
- [ ] `grep -n backend-killer README.md` показывает добавленные строки.

## Context Files

**Feature artifacts:**
- [001-feat-backend-killer.md](001-feat-backend-killer.md) — user-spec (раздел «Дизайн и интерфейс» — CLI-флаги; «Риски» — дисклеймер + impact=Yes)
- [001-feat-backend-killer-tech-spec.md](001-feat-backend-killer-tech-spec.md) — tech-spec (Task 3; Risks: очень большой `plan-size` делает каждый `PREPARE` тяжёлым/медленным)
- [001-feat-backend-killer-code-research.md](001-feat-backend-killer-code-research.md) — code research (раздел «README rows to add (exact tables)» — точные строки и формат)
- [001-feat-backend-killer-decisions.md](001-feat-backend-killer-decisions.md) — decisions log

**Project knowledge:**
- [project.md](../../../.claude/skills/project-knowledge/references/project.md)
- [architecture.md](../../../.claude/skills/project-knowledge/references/architecture.md)

**Code files:**
- [README.md](../../../README.md) — что изменить: bullet-список нагрузок и таблица Workload impact

## Verification Steps

- Шаг 1: `grep -n backend-killer README.md` — выводит и bullet-пункт, и строку impact-таблицы.
- Шаг 2: Визуально проверить, что bullet-пункт стоит среди остальных нагрузок (перед `...see
  built-in help`) и читается в том же стиле, что соседние пункты.
- Шаг 3: Визуально проверить, что строка impact-таблицы стоит в алфавитном порядке, имеет
  impact = **Yes** и не ломает markdown-разметку таблицы (выравнивание `| name | impact |`).
- Шаг 4: Убедиться, что упомянута оговорка про большой `plan-size`.
- Шаг 5: `git diff README.md` — изменения ограничены двумя местами (bullet-список + таблица),
  прочие секции не затронуты.

## Details

**Files:** `README.md` — две точки правки:

- Bullet-список поддерживаемых нагрузок (сейчас строки 7–16): нагрузки перечислены через
  `` - `name` - описание. ``; последний пункт — `- ...see built-in help for more runtime options.`
  Добавить пункт про `backend-killer` ПЕРЕД этим завершающим пунктом. Формулировку выровнять под
  соседей: одна сессия утечкой prepared statements (рост plan cache) раздувает память backend'а,
  пока он не получит OOM и инстанс не перезапустится; очень большой `--backend-killer.plan-size`
  делает каждый `PREPARE` тяжёлым/медленным.
- Таблица «Workload impact» (сейчас строки 79–88): заголовок `| Workload | Impact? |`, строки
  отсортированы по имени (`deadlocks`, `failconns`, `forkconns`, `idlexacts`, `rollbacks`,
  `tempfiles`, `terminate`, `waitxacts`). Имена в таблице — это имена ПАКЕТОВ (`forkconns`,
  `idlexacts`), не CLI-флаги с дефисом. Имя пакета новой нагрузки — `backendkiller` (см. tech-spec
  Decision 1). По алфавиту `backendkiller` идёт ПЕРВЫМ (перед `deadlocks`). Формат impact-ячейки
  как у `failconns`/`forkconns`: `**Yes**: <описание>`. Описание: одна сессия может растить RSS
  backend'а до OOM-kill и полного перезапуска инстанса; упомянуть, что большой `plan-size` делает
  `PREPARE` тяжёлым/медленным.

**Dependencies:** Зависимостей по задачам нет (`depends_on: []`) — README можно править параллельно
с Task 1/Task 2 в той же волне (wave 2). Содержательно задача опирается на факт существования
нагрузки и её флагов, уже описанных в user-spec/tech-spec.

**Edge cases:**
- Имя в bullet-списке использует пользовательский CLI-флаг `backend-killer` (с дефисом, как
  соседние описания «idle transactions», «fork connections»), а имя в impact-таблице — пакет
  `backendkiller` (без дефиса), как у других строк таблицы. Не перепутать два контекста.
- Не добавлять второй дисклеймер: секция Disclaimer уже покрывает разрушительность; impact=Yes
  достаточно для таблицы.

**Implementation hints:**
- Точные образцы строк (формат таблицы и bullet) есть в code-research, раздел «README rows to add
  (exact tables)». Использовать их как ориентир по формату, формулировку привести к стилю README.
- Сохранять существующий markdown-стиль таблицы (выравнивание разделителей `| :--- | :---: |`).

## Reviewers

- **dev-code-reviewer** → `001-feat-backend-killer-task-03-dev-code-reviewer-review.json`

## Post-completion

- [ ] Записать краткий отчёт в [001-feat-backend-killer-decisions.md](001-feat-backend-killer-decisions.md) (Summary: 1-3 предложения, ревью со ссылкой на JSON, без таблиц файндингов и дампов)
- [ ] Если отклонились от спека — описать отклонение и причину
- [ ] Обновить user-spec/tech-spec если что-то изменилось
