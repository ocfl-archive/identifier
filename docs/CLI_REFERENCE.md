# identifier CLI Command Reference

This document provides a comprehensive reference for all CLI commands, arguments, flags, and operational modes in **`identifier`**.

---

## Global Options

The following flags can be passed to `identifier` or any of its subcommands:

| Flag | Type | Default | Description |
|---|---|---|---|
| `--config <path>` | string | `""` | Path to custom TOML configuration file. If omitted, uses embedded default configuration. |
| `--log-file <path>` | string | `""` | File path to write structured logs. If omitted, logs to stderr. |
| `--log-level <level>` | string | `"WARN"` | Log verbosity: `ERROR`, `WARN`, `INFO`, `DEBUG`. |
| `--autoconfig` | bool | `false` | Enable automatic indexer performance optimization. |
| `--show-config` | bool | `false` | Prints loaded configuration TOML to stdout and exits immediately. |
| `-h`, `--help` | bool | `false` | Displays help message for command. |
| `-v`, `--version` | bool | `false` | Displays build version and VCS commit revision. |

---

## 1. Filename Sanitization: `identifier clearpath`

Cleans and standardizes problematic filenames bottom-up (deepest elements first).

### Syntax
```shell
identifier clearpath <path-to-data> [flags]
```

### Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--auto` | bool | `false` | Apply built-in archival filename cleaning rules. |
| `--rename` | bool | `false` | Execute renaming on disk. If omitted, runs in **dry-run mode** (preview only). |
| `--regexp <pattern>` | string | `""` | Custom Go regular expression to match within filenames. |
| `--replace <string>` | string | `""` | Replacement string for matches of `--regexp`. |

*(Note: `--auto` or `--regexp` is required. When `--regexp` is used, `--replace` is also required.)*

### Examples
```shell
# Preview built-in cleaning rules
identifier clearpath /data/estate --auto

# Apply built-in cleaning rules
identifier clearpath /data/estate --auto --rename

# Replace double periods with a single period
identifier clearpath /data/estate --regexp "\.\." --replace "." --rename
```

---

## 2. File Discovery & Removal: `identifier files`

Discovers files whose basename matches a regular expression, with optional deletion.

### Syntax
```shell
identifier files <path-to-data> --regexp <pattern> [flags]
```

### Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--regexp <pattern>` | string | `""` | **(Required)** Regular expression matched against file basenames. |
| `--remove` | bool | `false` | Deletes matched files from the filesystem. If omitted, runs in dry-run mode. |

### Examples
```shell
# List all hidden files starting with a dot
identifier files /data/estate --regexp "^\."

# Remove AppleDouble and OS X resource fork files
identifier files /data/estate --regexp "^\._" --remove
```

---

## 3. Folder Discovery & Removal: `identifier folders`

Discovers folders matching a regular expression and lists them. Matches terminate at the highest matching ancestor.

### Syntax
```shell
identifier folders <path-to-data> --regexp <pattern> [flags]
```

### Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--regexp <pattern>` | string | `""` | **(Required)** Regular expression matched against folder names. |
| `--remove` | bool | `false` | Recursively deletes matched folders and their contents from the filesystem. |

### Examples
```shell
# List empty or temporary folders
identifier folders /data/estate --regexp "^(tmp|temp|thumbs)$"

# Recursively remove matching folders
identifier folders /data/estate --regexp "^__MACOSX$" --remove
```

---

## 4. Technical Indexing: `identifier index`

Recursively scans a directory, identifies technical metadata (Siegfried, FFprobe, ImageMagick, Tika), computes SHA-512 hashes, detects duplicates, caches records in BadgerDB, and produces tabular exports.

### Syntax
```shell
identifier index <path-to-data> [flags]
```

### Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--database <dir>` | string | `""` | BadgerDB directory path for persistent caching. |
| `--concurrent <n>`, `-n <n>` | uint | `1` | Number of concurrent indexing worker goroutines. |
| `--actions <list>` | string slice | `siegfried,xml,ffprobe,identify,json,tika` | Indexer actions to run. |
| `--csv <path>` | string | `""` | Export metadata table to CSV file. |
| `--jsonl <path>` | string | `""` | Export metadata records to JSON Lines file. |
| `--xlsx <path>` | string | `""` | Export metadata table to Excel XLSX workbook. |
| `--console` | bool | `false` | Print indexed file metadata to console stdout. |

---

## 5. Querying Index Records: `identifier index list`

Queries and filters existing technical index records from BadgerDB.

### Syntax
```shell
identifier index list [path-to-data] --database <dir> [flags]
```

### Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--database <dir>` | string | `""` | **(Required)** BadgerDB storage directory. |
| `--duplicates` | bool | `false` | Filter for duplicate files (matching SHA-512 hashes). |
| `--empty` | bool | `false` | Filter for zero-byte files (`size == 0`). |
| `--regexp <pattern>` | string | `""` | Filter files whose basename matches regular expression. |
| `--prefix <path>` | string | `""` | Filter files located under a specific folder prefix. |
| `--remove` | bool | `false` | Delete matching files from disk and remove their database entries (requires `path-to-data`). |
| `--csv`, `--jsonl`, `--xlsx`, `--console` | | | Multi-format output options. |

---

## 6. Folder Statistics: `identifier index folders`

Calculates recursive file count, subfolder count, and total byte size per directory from BadgerDB.

### Syntax
```shell
identifier index folders --database <dir> [flags]
```

### Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--database <dir>` | string | `""` | **(Required)** BadgerDB storage directory. |
| `--prefix <path>` | string | `""` | Restrict aggregation to folders starting with prefix. |
| `--csv`, `--jsonl`, `--xlsx`, `--console` | | | Multi-format output options. |

---

## 7. Format Statistics: `identifier index mime` & `identifier index pronom`

Generates aggregated file count and byte size summaries grouped by MIME type or PRONOM ID.

### Syntax
```shell
identifier index mime --database <dir> [flags]
identifier index pronom --database <dir> [flags]
```

### Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--database <dir>` | string | `""` | **(Required)** BadgerDB storage directory. |
| `--duplicates` | bool | `false` | Restrict aggregation to duplicate files. |
| `--empty` | bool | `false` | Restrict aggregation to empty files. |
| `--regexp <pattern>` | string | `""` | Filter files before aggregating. |
| `--prefix <path>` | string | `""` | Filter folder prefix before aggregating. |
| `--csv`, `--jsonl`, `--xlsx`, `--console` | | | Multi-format output options. |

---

## 8. AI Semantic Appraisal: `identifier ai`

Executes Firebase Genkit structured flows against indexed folder trees to generate scholarly titles, descriptions, dates, places, persons, and institutions.

### Syntax
```shell
identifier ai --database <dir> [flags]
```

### Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--database <dir>` | string | `""` | **(Required)** BadgerDB storage directory. |
| `--model <provider/model>` | string | `"googleai/gemini-2.5-flash"` | Driver and model name (`googleai/...`, `openai/...`, `ceda/...`). |
| `--apikey <key>` | string | `"%%GEMINI_API_KEY%%"` | API key or environment variable reference. |
| `--query <path\|str>` | string | `""` | Custom prompt query text or path to prompt text file. |
| `--additional-query <path\|str>` | string | `""` | Additional prompt instructions prepended to query. |
| `--max-files <n>` | int64 | `8` | Maximum number of sample files per folder sent to LLM. |
| `--result-folder <n>` | int64 | `50` | Batch size of folders sent per LLM prompt (`0` = all). |
| `--prefix <path>` | string | `""` | Restrict AI description generation to folders with prefix. |
| `--csv`, `--jsonl`, `--xlsx`, `--console` | | | Multi-format output options. |

---

## 9. Querying Stored AI Descriptions: `identifier ai list`

Retrieves cached AI descriptions from BadgerDB without invoking the LLM API.

### Syntax
```shell
identifier ai list --database <dir> [flags]
```

### Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--database <dir>` | string | `""` | **(Required)** BadgerDB storage directory. |
| `--prefix <path>` | string | `""` | Restrict retrieval to folders with prefix. |
| `--csv`, `--jsonl`, `--xlsx`, `--console` | | | Multi-format output options. |

---

## 10. RO-Crate Packaging: `identifier ai ro-crate`

Embeds AI descriptions and hierarchy structure into an RO-Crate 1.1 JSON-LD metadata document (`ro-crate-metadata.json`).

### Syntax
```shell
identifier ai ro-crate <path-to-data> --database <dir> [flags]
```

### Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--database <dir>` | string | `""` | **(Required)** BadgerDB storage directory. |
| `--model <name>` | string | `"googleai/gemini-2.5-flash"` | Model name used to store AI descriptions. |
| `--prefix <path>` | string | `""` | Subfolder prefix for RO-Crate target root. |
