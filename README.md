# Match job requirements to book sections

Pipeline for **staging job postings** (JSON) and **EPUB books** into a shared **DuckDB** database with **sentence-transformer** vector embeddings, plus **SQL** examples under `script/sql/`.

## Layout

| Path                                                                     | Purpose                                                                                                                                                                            |
| ------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `data/`                                                                  | Staging JSON inputs and `jds_books.duckdb` (default DB). Keep large/local DBs gitignored.                                                                                          |
| `books/`                                                                 | Add `.epub` files here for batch ingest; see `books/README.txt`.                                                                                                                   |
| `script/`                                                                | Main Python tools (`embed_staging_jd_duckdb.py`, `embed_staging_books.py`, helpers).                                                                                               |
| `script/sql/`                                                            | DuckDB SQL (e.g. `transformations.sql` — run with repo root as `cwd` or fix `ATTACH` paths).                                                                                       |
| `archive/chromadb/`                                                      | Legacy Chroma-based ingest; optional and usually **not** in git.                                                                                                                   |
| `archive/script/build_session_json.py`, `archive/data/descriptions_raw/` | Legacy manual flow: merge fixed job metadata with per-job `.txt` raw descriptions. **Superseded** by `script/scrape_linkedin_jobs.py`, which puts descriptions in the scrape JSON. |

## Setup

1. **Python 3.10+** recommended.  
2. Create and activate a venv (e.g. `script/setup_venv.ps1` on Windows).  
3. Install runtime deps from `script/requirements-vector.txt` (includes DuckDB, sentence-transformers, EPUB/HTML stack, and **Playwright** for LinkedIn scraping). You still need a PyTorch build suitable for your machine for `sentence-transformers`.

**Optional environment variables** (read by `script/local_paths.py`; defaults are repo `data/` and `books/`)

| Variable              | Purpose                                                              |
| --------------------- | -------------------------------------------------------------------- |
| `JD_STAGING_DATA_DIR` | Directory that contains `jds_books.duckdb` and related staging data. |
| `JD_BOOKS_DIR`        | Directory scanned for `*.epub` batch ingest.                         |

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

## LinkedIn Scrape Usage

`script/scrape_linkedin_jobs.py` collects LinkedIn job cards plus each job's "About the job" text and writes one JSON payload.

Install once in your venv (same file `script/setup_venv.ps1` copies to the repo root when you run it):

```bash
pip install -r script/requirements-vector.txt
python -m playwright install chromium
```

`script/requirements-vector.txt` lists the `playwright` package; **`python -m playwright install chromium`** downloads the Chromium browser binaries (run again after recreating the venv or on a new machine).

Recommended first run (headed, persistent profile so you can stay logged in):

```bash
python script/scrape_linkedin_jobs.py --headed --user-data-dir ./.pw-linkedin-profile --pause-after-load
```

Common examples:

```bash
# Default output: data/source_jd/YYYY-MM-DD/<epoch>/linkedin_jobs.json
python script/scrape_linkedin_jobs.py --out data/custom_run.json
python script/scrape_linkedin_jobs.py --keywords "Data Engineer" --location "Edison, NJ" --distance-mi 50 --days week --max-jobs 20
python script/scrape_linkedin_jobs.py --headless --user-data-dir ./.pw-linkedin-profile
```

Key flags:

- `--keywords`, `--location`, `--distance-mi`, `--days` (`day|week|month`) control the search.
- `--max-jobs` caps collected postings.
- `--out` overrides the output JSON path (default: `data/source_jd/YYYY-MM-DD/<epoch>/linkedin_jobs.json`).
- `--headed`/`--headless` controls browser visibility.
- `--user-data-dir` reuses Chromium profile/cookies to reduce login friction.
- `--pause-after-load` waits for Enter after the search page opens so you can finish sign-in before scraping (avoids the browser closing while you type when the job list is still empty).
- `--slow-mo` adds delay per browser action for debugging.

Output shape includes search metadata and `jobs[]` records with `job_id`, `title`, `company`, `location`, `url`, and extracted `description`.

## Job postings → DuckDB

From `script/` with the venv active:

```bash
python embed_staging_jd_duckdb.py --reset --min-merge-chars 10
```

Default input: latest `data/source_jd/<YYYY-MM-DD>/<epoch>/linkedin_jobs.json` for today UTC (override with `--input` file or date folder, or `--scrape-date`).
Table: staging_jd in data/jds_books.duckdb (paths configurable; see local_paths / duckdb_connect).

## Books (EPUB) → DuckDB

```bash
python embed_staging_books.py
python embed_staging_books.py --headers-only
python embed_staging_books.py --epub "../books/Your Book.epub"
```

**EPUB scripts:** `script/staging_books_epub.py` is the **chunking library** only — it reads EPUBs (via `ebooklib` / HTML parsing), applies filters (non-chapter spine items, footnotes, code blocks, etc.), and yields `BookChunk` records (text + ISBN, titles, section indices). It does **not** talk to DuckDB or compute embeddings. `script/embed_staging_books.py` is the **ingest driver**: it imports `iter_epub_chunks` from that module, runs **sentence-transformers**, and inserts rows into the **`staging_books`** table in `jds_books.duckdb` (batch from `books/` or a single `--epub`, with `--reset` / resume behavior documented in its docstring).

Table: staging_books in the same DuckDB file. See the script’s docstring for --reset, resume, and limits.

## SQL

script/sql/transformations.sql — example joins / similarity ideas. Use DuckDB with cwd at the repo root so data/jds_books.duckdb matches the ATTACH in the file, or edit paths.

## Sample Report

![Sample report](docs/sample_report.png)
