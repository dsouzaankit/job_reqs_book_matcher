# job_reqs_book_matcher

Pipeline for **staging job postings** (JSON) and **EPUB books** into a shared **DuckDB** database with **sentence-transformer** vector embeddings, plus **SQL** examples under `script/sql/`.

## Layout

| Path | Purpose |
|------|---------|
| `data/` | Staging JSON inputs and `jds_books.duckdb` (default DB). Keep large/local DBs gitignored. |
| `books/` | Add `.epub` files here for batch ingest; see `books/README.txt`. |
| `script/` | Main Python tools (`embed_staging_jd_duckdb.py`, `embed_staging_books.py`, helpers). |
| `script/sql/` | DuckDB SQL (e.g. `transformations.sql` — run with repo root as `cwd` or fix `ATTACH` paths). |
| `archive/chromadb/` | Legacy Chroma-based ingest; optional and usually **not** in git. |
| `archive/script/build_session_json.py`, `archive/data/descriptions_raw/` | Legacy manual flow: merge fixed job metadata with per-job `.txt` raw descriptions. **Superseded** by `script/scrape_linkedin_jobs.py`, which puts descriptions in the scrape JSON. |

## Setup

1. **Python 3.10+** recommended.  
2. Create and activate a venv (e.g. `script/setup_venv.ps1` on Windows).  
3. Install runtime deps: `duckdb`, `sentence-transformers`, `numpy`, and a PyTorch build suitable for your machine (and any other imports your stack uses). Add a `requirements.txt` if you want pinned versions.

**Optional environment variables** (read by `script/local_paths.py`; defaults are repo `data/` and `books/`)

| Variable | Purpose |
|----------|---------|
| `JD_STAGING_DATA_DIR` | Directory that contains `jds_books.duckdb` and related staging data. |
| `JD_BOOKS_DIR` | Directory scanned for `*.epub` batch ingest. |

**Windows — PowerShell (current session only)**

```powershell
$env:JD_STAGING_DATA_DIR = "D:\my-data\job_matcher"
$env:JD_BOOKS_DIR = "D:\epubs"
```

Run your Python commands from the same terminal. Paths should exist (or your workflow should create them).

**Windows — persist for your user**

```powershell
[System.Environment]::SetEnvironmentVariable("JD_STAGING_DATA_DIR", "D:\my-data\job_matcher", "User")
[System.Environment]::SetEnvironmentVariable("JD_BOOKS_DIR", "D:\epubs", "User")
```

Restart the terminal (and Cursor) so new processes see the values.

**Windows (GUI):** Run `sysdm.cpl`, open the **Advanced** tab, click **Environment Variables**, then under **User** variables click **New** for each name and directory path.


## Job postings → DuckDB

From `script/` with the venv active:

```bash
python embed_staging_jd_duckdb.py --reset --min-merge-chars 10
```

Default input: ../data/linkedin_data_engineer_edison_50mi.json (override with --input).
Table: staging_jd in data/jds_books.duckdb (paths configurable; see local_paths / duckdb_connect).


## Books (EPUB) → DuckDB

```bash
python embed_staging_books.py
python embed_staging_books.py --headers-only
python embed_staging_books.py --epub "../books/Your Book.epub"
```

Table: staging_books in the same DuckDB file. See the script’s docstring for --reset, resume, and limits.

## SQL

script/sql/transformations.sql — example joins / similarity ideas. Use DuckDB with cwd at the repo root so data/jds_books.duckdb matches the ATTACH in the file, or edit paths.

## Sample Report

![Sample report](docs/sample_report.png)
