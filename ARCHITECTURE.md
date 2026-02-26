# SQLite mini-engine architecture (for CodeCrafters stages)

This document outlines a **high-level, non-bespoke** architecture for implementing a small SQLite engine that supports simple `SELECT` queries in a TDD-friendly way.

## 1) Pipeline overview

Use a clear pipeline with strict boundaries:

1. **Storage layer**: read SQLite file structures (header, pages, b-tree cells, records)
2. **Catalog layer**: resolve schema metadata (tables, columns, indices) from `sqlite_master`
3. **Logical planning**: convert parsed SQL AST into a small logical plan IR
4. **Physical planning**: choose concrete operators (table scan vs index lookup)
5. **Execution engine**: iterator-based operators produce rows
6. **Output layer**: format rows per challenge requirements

This separation prevents stage-specific hacks from leaking into unrelated concerns.

## 2) Minimal intermediate representations

Keep IRs tiny and evolvable.

### 2.1 Logical plan IR (what to compute)

```text
LogicalPlan
  - Scan(table)
  - Filter(input, predicate)
  - Project(input, exprs)
  - Limit(input, n)               // optional, future-friendly
```

With CodeCrafters early stages, this is often enough:
- `SELECT col1, col2 FROM t`
- `SELECT ... FROM t WHERE col = literal`

### 2.2 Physical plan IR (how to compute)

```text
PhysicalPlan
  - TableScan(table, columnMap)
  - IndexScan(index, lookupKey)   // when available/useful
  - FilterExec(input, compiledPredicate)
  - ProjectExec(input, compiledExprs)
```

### 2.3 Runtime row/value model

Use stable internal data types to avoid parser/storage coupling:

- `Value`: `Null | Integer | Real | Text | Blob`
- `Row`: positional tuple (`[]Value`) + optional schema handle
- `Schema`: column descriptors with name + affinity

## 3) Execution model: Volcano iterator

Use the classic pull-based iterator interface for operators:

```text
Open() error
Next() (Row, bool, error)   // bool=false means EOF
Close() error
```

Why this is ideal here:
- simple to reason about in tests
- composes naturally (`Project(Filter(Scan(...)))`)
- no need for global materialization

## 4) Predicate/expression compilation

Don’t evaluate AST directly inside scans. Compile once.

- Parse AST (library)
- Build expression nodes in your own IR (`ColumnRef`, `Literal`, `Eq`, etc.)
- Compile expression IR into small evaluators:
  - `func(Row) (Value, error)` for scalar expressions
  - `func(Row) (bool, error)` for predicates

This keeps parser choice replaceable and makes tests deterministic.

## 5) Storage and access boundaries

Define interfaces so planner/executor are storage-agnostic:

```text
TableReader:
  Scan(tableName) -> RowIterator
  LookupByRowID(tableName, rowid) -> Row

Catalog:
  GetTable(name) -> TableMeta
  GetIndex(table, column) -> IndexMeta?
```

Implement these with SQLite file readers behind the scenes:
- decode b-tree pages/cells
- decode record serial types into `Value`
- expose rows in table-column order

## 6) Suggested package/module layout (Go)

```text
internal/
  storage/      // page I/O, b-tree traversal, record decoding
  catalog/      // sqlite_master loading, table/index metadata
  sqlplan/      // logical plan + expression IR
  optimizer/    // logical -> physical (very small rule set)
  exec/         // iterator operators and expression evaluators
  engine/       // orchestration: parse -> plan -> execute
```

Your `main` should mostly wire dependencies and run the engine.

## 7) TDD strategy per stage

For each new stage, add tests in this order:

1. **Planner tests**: SQL AST -> logical plan shape
2. **Optimizer tests**: logical -> physical choice
3. **Operator unit tests**: filter/project semantics with synthetic rows
4. **Storage integration tests**: known sample DB -> expected rows
5. **End-to-end CLI test**: command output

This order avoids overfitting to output-only checks.

## 8) Anti-bespoke rules (guardrails)

- Never branch on raw SQL strings in execution paths.
- Never read from parser AST directly in operators.
- Never combine file-format decoding and SQL semantics in one function.
- Keep one-way dependencies:
  - `storage -> catalog -> planner/optimizer -> exec -> CLI`

## 9) A practical MVP to implement next

1. Implement `Value`, `Row`, `Schema`
2. Implement `Scan`, `Filter`, `Project` logical nodes
3. Implement iterator operators for `TableScan`, `FilterExec`, `ProjectExec`
4. Add tiny optimizer rule:
   - `Filter(Scan(table), col = literal)` -> `IndexScan` when index exists, else table scan
5. Keep parser adapter thin: AST -> your expression/logical nodes

This gives you a maintainable base that can grow from early CodeCrafters stages to more complete query support without rewrites.

## 10) Mapping this architecture to your **current codebase**

Below is where your existing files fit today.

### 10.1 Storage layer (mostly present)

- `internal/db/file.go`
  - opens database file
  - reads database header
  - loads a page by page number
- `internal/db/page.go`
  - page bounds
  - page header decoding (`PageType`, `CellCount`, cell pointer array)
- `internal/db/row.go`
  - cell decoding
  - varint/serial-type interpretation
  - row payload decoding into Go values

This is your strongest layer right now.

### 10.2 Catalog layer (partially present)

- `internal/db/schema.go`
  - decodes rows from `sqlite_schema` (via page 1)
  - `ExtractTableNames`
  - `RootPageLookup`

This acts as a minimal catalog, but it is still mixed into `db` and tightly coupled to raw rows.

### 10.3 Logical planning layer (very thin today)

- `internal/engine/query.go`
  - `TableNameFromQuery` parses SQL and extracts only table name from `FROM`

This is not yet a real logical planner. It is currently an AST helper.

### 10.4 Physical planning layer (missing)

- No explicit physical plan exists yet.
- `RowCount` in `internal/engine/query.go` effectively hard-codes one “plan”:
  - look up root page
  - read page header
  - return `CellCount`

This bypasses any operator abstraction.

### 10.5 Execution layer (missing as a separate abstraction)

- There is no iterator/operator interface yet (`Open/Next/Close`).
- “Execution” is currently direct imperative logic inside `engine.RowCount`.

### 10.6 Output/CLI layer (present)

- `app/main.go` command routing
- `internal/cli/commands.go` output formatting for `.dbinfo`, `.tables`, and query count

This is fine, but query handling should call a stable engine API once planning/execution are introduced.

## 11) What to build in the **two planning layers** (concrete, incremental)

Treat planning as two explicit steps:

1. SQL AST -> **LogicalPlan** (semantic shape)
2. LogicalPlan -> **PhysicalPlan** (concrete operators/access path)

### 11.1 Logical planning: responsibilities and output

Given parser output, produce a tiny logical plan tree:

```text
Project(exprs)
  Filter(predicate)
    Scan(table)
```

Rules for early CodeCrafters stages:

- Must validate: exactly one table in `FROM`
- Build `Scan(tableName)`
- If `WHERE` exists (equality is enough initially), wrap in `Filter`
- Build projection list from `SELECT` expressions (`*` or column refs first)
- Do **not** choose index/table access here

Output should use your own nodes (not parser AST nodes) so parser changes won’t leak.

### 11.2 Physical planning: responsibilities and output

Input: logical plan + catalog metadata.

Output: executable operators, e.g.

```text
ProjectExec
  FilterExec
    TableScanExec
```

or

```text
ProjectExec
  IndexLookupExec
```

Early-stage decision rules can stay very small:

1. If plan root is `Scan(table)` -> `TableScanExec(table)`
2. If `Filter(Scan(table), col = literal)` and suitable index metadata exists -> `IndexLookupExec`
3. Else -> `FilterExec(TableScanExec(...))`

Physical planning is where access-path choice belongs; keep logical planner unaware of this.

### 11.3 How this maps to your current functions

- `TableNameFromQuery` should become part of a broader `BuildLogicalPlan(query)` function.
- `RootPageLookup` should be called from catalog/physical planning, not from CLI-level query handlers.
- `RowCount` should eventually disappear into execution operators (for `COUNT(*)`, use an aggregate operator later; for now, keep dedicated behavior but route through plans).

### 11.4 Minimal next refactor sequence (low risk)

1. Add logical node structs (`Scan`, `Filter`, `Project`) and expression nodes (`ColumnRef`, `Literal`, `Eq`).
2. Replace `TableNameFromQuery` with a logical planner that returns a plan tree.
3. Add physical node structs (`TableScanExec`, `FilterExec`, `ProjectExec`) plus iterator interface.
4. Add a tiny physical planner that lowers logical -> physical using catalog metadata.
5. Make `cli.HandleQuery` call a single engine entrypoint: `Execute(path, query)`.

If you do only the above, you’ll already remove most bespoke behavior while keeping changes stage-friendly.
