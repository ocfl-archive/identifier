# identifier Architecture & Component Design

This document details the internal design, concurrency model, data structures, and storage subsystems of **`identifier`**.

---

## Component Architecture

```mermaid
graph TB
    subgraph CLI["CLI Subsystem (Cobra)"]
        CmdRoot["rootCmd"]
        CmdClear["clearpathCmd"]
        CmdFiles["filesCmd / foldersCmd"]
        CmdIndex["indexCmd & subcommands (list, folders, mime, pronom)"]
        CmdAI["aiCmd & subcommands (list, ro-crate)"]
    end

    subgraph Core["identifier Core Engine"]
        PathTree["pathElement Hierarchy Tree<br/>(Depth-First Traversals & Aggregation)"]
        WorkerPool["Concurrent Index Worker Pool<br/>(SHA-512 & Format Actions)"]
        MultiOut["Output Dispatcher<br/>(CSV, JSONL, XLSX, Console)"]
        RoCrateEngine["RO-Crate 1.1 JSON-LD Graph Engine"]
    end

    subgraph Storage["Storage & Cache"]
        BadgerKV["BadgerDB Key-Value Store<br/>(file:* and ai:model:folder records)"]
    end

    subgraph AI["AI Subsystem (Firebase Genkit)"]
        GenkitFlow["Genkit Flow: identifier<br/>(DefineFlow with JSON Schema)"]
        GenkitPlugins["Plugins: GoogleGenAI / OpenAI / CEDA LiteLLM"]
    end

    CmdRoot --> CmdClear
    CmdRoot --> CmdFiles
    CmdRoot --> CmdIndex
    CmdRoot --> CmdAI

    CmdClear --> PathTree
    CmdFiles --> PathTree
    CmdIndex --> WorkerPool
    WorkerPool --> BadgerKV
    WorkerPool --> MultiOut
    CmdIndex --> MultiOut

    CmdAI --> GenkitFlow
    GenkitFlow --> GenkitPlugins
    CmdAI --> BadgerKV
    CmdAI --> RoCrateEngine
    RoCrateEngine --> MultiOut
```

---

## Data Structures

### 1. `FileData` (`identifier/structures.go`)

Encapsulates complete filesystem attributes and technical characterization results for an individual file:

```go
type FileData struct {
    Path      string            `json:"path,omitempty"`      // Relative slash-separated path
    Folder    string            `json:"folder,omitempty"`    // Containing relative folder
    Basename  string            `json:"basename,omitempty"`  // Filename
    Size      int64             `json:"size,omitempty"`      // Size in bytes
    Duplicate bool              `json:"duplicate,omitempty"` // Flagged if SHA-512 matches earlier file
    LastMod   int64             `json:"lastmod,omitempty"`   // File modification Unix timestamp
    Indexer   *indexer.ResultV2 `json:"indexer,omitempty"`   // Technical indexer results
    LastSeen  int64             `json:"lastseen,omitempty"`  // Scan timestamp
}
```

### 2. `AIResultStruct` (`identifier/structures.go`)

Holds LLM-generated descriptive metadata for a folder:

```go
type AIResultStruct struct {
    Folder       string     `json:"folder"`
    Title        string     `json:"title"`
    Description  string     `json:"description"`
    Place        string     `json:"place,omitempty"`
    Date         string     `json:"date,omitempty"`
    Tags         []string   `json:"tags,omitempty"`
    Persons      []AIPerson `json:"persons,omitempty"`
    Institutions []string   `json:"institutions,omitempty"`
}
```

### 3. `pathElement` (`identifier/clearpath.go`)

An in-memory n-ary tree node representing files and directories for recursive aggregation, regex queries, and post-order traversal renaming:

```mermaid
classDiagram
    class pathElement {
        -string name
        -string clearName
        -bool dir
        -int64 size
        -pathElement parent
        -pathElement[] subs
        +AddSub(name, dir, size) pathElement
        +SubFolderHierarchyAggregation() (size, fileCount, folderCount)
        +ClearIterator(auto, regex, replace) Iterator
        +FindBasename(regex) Iterator
        +FindDirname(regex) Iterator
    }
```

---

## BadgerDB Storage Architecture

`identifier` uses BadgerDB as an embedded, LSM-tree-based, zero-allocation key-value store with Snappy compression.

```mermaid
erDiagram
    FILE_RECORD {
        string key "file:<relative-path>"
        string value "JSON FileData"
    }
    AI_RECORD {
        string key "ai:<model-name>:<relative-folder>"
        string value "JSON AIResultStruct"
    }
```

### Key Formats & Semantics:
- **`file:<relative_path>`**: Stores technical metadata indexed from the filesystem. If a file's size and modification timestamp match an existing record during re-indexing, the cached index result is loaded instantly.
- **`ai:<model_name>:<relative_folder>`**: Stores structured semantic descriptions produced by LLMs. Namespacing by model allows comparative evaluation across different models without data loss.

---

## Genkit Flow Execution Pipeline

```mermaid
sequenceDiagram
    autonumber
    participant Bad as BadgerDB
    participant AI as identifier ai
    participant Flow as Genkit DefineFlow
    participant Plugin as GoogleAI / OpenAI / LiteLLM

    AI->>Bad: Read all file:* keys
    AI->>AI: Group files into folderT map and sample excerpts
    loop For each batch (default: 50 folders)
        AI->>Flow: Run(context, []*folderT)
        Flow->>Flow: Construct prompt (INPUT-JSON + FOLDERLIST-JSON)
        Flow->>Plugin: GenerateData[resultList](prompt)
        Plugin-->>Flow: Typed resultList (JSON parsed into Go structs)
        Flow-->>AI: *resultList
        AI->>Bad: Set ai:<model>:<folder>
        AI->>AI: Dispatch to MultiOutput (CSV, JSONL, XLSX, Console)
    end
```
