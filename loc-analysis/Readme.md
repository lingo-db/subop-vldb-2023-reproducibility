For LingoDB:
```bash
cd lingo-db
cloc --sql 1 --force-lang-def cloc.defs lib include | sqlite3 stats.db
sqlite3 stats.db < lingodb-locs.sql
```

For DuckDB:
```bash
git clone https://gitlab.db.in.tum.de/jungmair/duckdb-reduced.git
cd duckdb-reduced
cloc --sql 1 --force-lang-def cloc.defs src | sqlite3 stats.db
sqlite3 stats.db < duckdb-locs.sql
```