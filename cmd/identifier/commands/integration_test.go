package commands

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ocfl-archive/identifier/identifier"
	"github.com/tealeg/xlsx/v3"
)

func resetAllFlags() {
	identifier.ResetHashes()

	persistentFlagConfigFile = ""
	persistentFlagLogfile = ""
	persistentFlagLoglevel = "WARN"
	persistentFlagAutoconfig = false
	showConfig = false

	clearPathRenameFlag = false
	clearPathAutoFlag = false
	clearPathRegexpFlag = ""
	clearPathRegexpReplaceFlag = ""
	clearPathRegexp = nil

	filesRemoveFlag = false
	filesRegexpFlag = ""

	foldersRemoveFlag = false
	foldersRegexpFlag = ""

	dbFolderFlag = ""
	csvFlag = ""
	jsonlFlag = ""
	xlsxFlag = ""
	consoleFlag = false
	concurrentFlag = 2
	actionsFlag = []string{"siegfried", "xml", "json"}

	csvIndexListFlag = ""
	jsonlIndexListFlag = ""
	xlsxIndexListFlag = ""
	dbFolderIndexListFlag = ""
	emptyIndexListFlag = false
	regexpIndexListFlag = ""
	duplicatesIndexListFlag = false
	prefixIndexListFlag = ""
	removeIndexListFlag = false
	consoleIndexListFlag = false

	csvIndexFolderFlag = ""
	jsonlIndexFolderFlag = ""
	xlsxIndexFolderFlag = ""
	consoleIndexFolderFlag = false
	prefixIndexFolderFlag = ""
	dbIndexFolderFlag = ""

	csvIndexMimeFlag = ""
	jsonlIndexMimeFlag = ""
	xlsxIndexMimeFlag = ""
	dbFolderIndexMimeFlag = ""
	emptyIndexMimeFlag = false
	regexpIndexMimeFlag = ""
	duplicatesIndexMimeFlag = false
	prefixIndexMimeFlag = ""
	consoleIndexMimeFlag = false

	csvIndexPronomFlag = ""
	jsonlIndexPronomFlag = ""
	xlsxIndexPronomFlag = ""
	dbFolderIndexPronomFlag = ""
	emptyIndexPronomFlag = false
	regexpIndexPronomFlag = ""
	duplicatesIndexPronomFlag = false
	prefixIndexPronomFlag = ""
	consoleIndexPronomFlag = false
}

func execCommand(t *testing.T, args ...string) (string, error) {
	t.Helper()
	resetAllFlags()
	buf := new(bytes.Buffer)
	rootCmd.SetOut(buf)
	rootCmd.SetErr(buf)
	rootCmd.SetArgs(args)
	err := rootCmd.Execute()
	return buf.String(), err
}

func mustExecCommand(t *testing.T, args ...string) string {
	t.Helper()
	out, err := execCommand(t, args...)
	if err != nil {
		t.Fatalf("command [%s] failed with error %v\nOutput: %s", strings.Join(args, " "), err, out)
	}
	return out
}

// createTestDataset builds a multi-level directory with various edge cases:
// - Illegal/PUA characters, double dots, em-dashes, multi-spaces (for clearpath testing)
// - Temporary / hidden files (for files removal testing)
// - Empty folders (for folders removal testing)
// - Multiple valid documents (XML, JSON, TXT)
// - Duplicate files across different subdirectories (for deduplication testing)
// - Empty files (0-byte)
func createTestDataset(t *testing.T, baseDir string) {
	t.Helper()

	// 1. Unsanitized directory with dirty/PUA characters
	dirtyDir := filepath.Join(baseDir, "dirty_dir")
	if err := os.MkdirAll(dirtyDir, 0755); err != nil {
		t.Fatalf("failed to create dirty directory: %v", err)
	}

	// File with PUA characters: \uE001 (should be stripped by clearpath)
	puaFile := filepath.Join(dirtyDir, "pua\uE001file.xml")
	if err := os.WriteFile(puaFile, []byte(`<?xml version="1.0"?><data><id>100</id><desc>PUA test</desc></data>`), 0644); err != nil {
		t.Fatalf("failed to write PUA file: %v", err)
	}

	// File with em-dash: em—dash.txt (should be normalized to em-dash.txt)
	dashFile := filepath.Join(dirtyDir, "em\u2014dash.txt")
	if err := os.WriteFile(dashFile, []byte("Content with em-dash filename."), 0644); err != nil {
		t.Fatalf("failed to write dash file: %v", err)
	}

	// File with double dot prefix: ..prefix.json (should become _.prefix.json)
	dotFile := filepath.Join(dirtyDir, "..prefix.json")
	if err := os.WriteFile(dotFile, []byte(`{"status": "ok", "version": "1.0"}`), 0644); err != nil {
		t.Fatalf("failed to write double dot file: %v", err)
	}

	// File with multiple spaces: multi   spaces.txt (should become multi spaces.txt)
	spaceFile := filepath.Join(dirtyDir, "multi   spaces.txt")
	if err := os.WriteFile(spaceFile, []byte("Content with consecutive spaces."), 0644); err != nil {
		t.Fatalf("failed to write space file: %v", err)
	}

	// 2. Hidden and temporary files / empty folders for cleanup testing
	hiddenDir := filepath.Join(baseDir, ".hidden_dir")
	if err := os.MkdirAll(hiddenDir, 0755); err != nil {
		t.Fatalf("failed to create hidden directory: %v", err)
	}
	dsStore := filepath.Join(baseDir, ".DS_Store")
	if err := os.WriteFile(dsStore, []byte("mock macos metadata"), 0644); err != nil {
		t.Fatalf("failed to write .DS_Store: %v", err)
	}
	tmpFile := filepath.Join(hiddenDir, "backup.tmp")
	if err := os.WriteFile(tmpFile, []byte("temporary backup content"), 0644); err != nil {
		t.Fatalf("failed to write tmp file: %v", err)
	}

	// Empty folder structure
	emptyDir := filepath.Join(baseDir, "empty_artifacts", "nested_empty")
	if err := os.MkdirAll(emptyDir, 0755); err != nil {
		t.Fatalf("failed to create empty nested directories: %v", err)
	}

	// 3. Normal files and structured documents
	docsDir := filepath.Join(baseDir, "documents")
	if err := os.MkdirAll(docsDir, 0755); err != nil {
		t.Fatalf("failed to create documents directory: %v", err)
	}

	articleContent := []byte("Digital preservation repository identifier package integration test data content.")
	articleFile := filepath.Join(docsDir, "article.txt")
	if err := os.WriteFile(articleFile, articleContent, 0644); err != nil {
		t.Fatalf("failed to write article: %v", err)
	}

	xmlContent := []byte(`<?xml version="1.0" encoding="UTF-8"?><library><book id="1"><title>Go Testing</title></book></library>`)
	xmlFile := filepath.Join(docsDir, "catalog.xml")
	if err := os.WriteFile(xmlFile, xmlContent, 0644); err != nil {
		t.Fatalf("failed to write XML: %v", err)
	}

	jsonContent := []byte(`{"app": "identifier", "features": ["checksum", "pronom", "mime"]}`)
	jsonFile := filepath.Join(docsDir, "config.json")
	if err := os.WriteFile(jsonFile, jsonContent, 0644); err != nil {
		t.Fatalf("failed to write JSON: %v", err)
	}

	// Empty 0-byte file
	emptyFile := filepath.Join(docsDir, "empty_file.txt")
	if err := os.WriteFile(emptyFile, []byte{}, 0644); err != nil {
		t.Fatalf("failed to write empty file: %v", err)
	}

	// 4. Duplicate files across different folders
	subDir1 := filepath.Join(baseDir, "duplicates_folder_a")
	subDir2 := filepath.Join(baseDir, "duplicates_folder_b")
	if err := os.MkdirAll(subDir1, 0755); err != nil {
		t.Fatalf("failed to create subDir1: %v", err)
	}
	if err := os.MkdirAll(subDir2, 0755); err != nil {
		t.Fatalf("failed to create subDir2: %v", err)
	}

	dupArticle1 := filepath.Join(subDir1, "article_duplicate_1.txt")
	if err := os.WriteFile(dupArticle1, articleContent, 0644); err != nil {
		t.Fatalf("failed to write duplicate 1: %v", err)
	}

	dupArticle2 := filepath.Join(subDir2, "article_duplicate_2.txt")
	if err := os.WriteFile(dupArticle2, articleContent, 0644); err != nil {
		t.Fatalf("failed to write duplicate 2: %v", err)
	}

	dupXml := filepath.Join(subDir2, "catalog_duplicate.xml")
	if err := os.WriteFile(dupXml, xmlContent, 0644); err != nil {
		t.Fatalf("failed to write XML duplicate: %v", err)
	}
}

// readJSONLRecords parses a JSONL output file into a slice of FileData pointers.
func readJSONLRecords(t *testing.T, path string) []*identifier.FileData {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open jsonl file %s: %v", path, err)
	}
	defer f.Close()

	var records []*identifier.FileData
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var record identifier.FileData
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("failed to unmarshal JSONL line: %s, err: %v", line, err)
		}
		records = append(records, &record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("error scanning jsonl: %v", err)
	}
	return records
}

// readCSVRecords reads and returns all rows from a CSV file.
func readCSVRecords(t *testing.T, path string) [][]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open csv file %s: %v", path, err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("failed to read csv %s: %v", path, err)
	}
	return records
}

func TestEndToEndArchivalWorkflowIntegration(t *testing.T) {
	// Initialize test directories
	workDir := t.TempDir()
	dataDir := filepath.Join(workDir, "dataset")
	dbDir := filepath.Join(workDir, "badger_db")
	reportsDir := filepath.Join(workDir, "reports")

	if err := os.MkdirAll(reportsDir, 0755); err != nil {
		t.Fatalf("failed to create reports directory: %v", err)
	}

	t.Log("=== Phase 0: Testdatensatz erstellen ===")
	createTestDataset(t, dataDir)

	// =========================================================================
	// Phase 1: Bereinigung unerwünschter Artefakte (files & folders)
	// =========================================================================
	t.Log("=== Phase 1.1: files Suche & Löschen von temporären/versteckten Dateien ===")
	dsStorePath := filepath.Join(dataDir, ".DS_Store")
	if _, err := os.Stat(dsStorePath); os.IsNotExist(err) {
		t.Fatalf("expected .DS_Store to exist before removal")
	}

	// Dry-run check: verify dry-run does NOT delete .DS_Store
	_ = mustExecCommand(t, "files", dataDir, "--regexp", `^\.DS_Store`)
	if _, err := os.Stat(dsStorePath); os.IsNotExist(err) {
		t.Fatalf("files dry-run should not delete .DS_Store")
	}

	// Real deletion of .DS_Store
	_ = mustExecCommand(t, "files", dataDir, "--regexp", `^\.DS_Store`, "--remove")
	if _, err := os.Stat(dsStorePath); !os.IsNotExist(err) {
		t.Fatalf(".DS_Store should have been deleted")
	}

	// Real deletion of .tmp backup files
	tmpPath := filepath.Join(dataDir, ".hidden_dir", "backup.tmp")
	if _, err := os.Stat(tmpPath); os.IsNotExist(err) {
		t.Fatalf("expected backup.tmp to exist before removal")
	}
	_ = mustExecCommand(t, "files", dataDir, "--regexp", `\.tmp$`, "--remove")
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatalf("backup.tmp should have been deleted")
	}

	t.Log("=== Phase 1.2: folders Löschen leerer Verzeichnisse ===")
	emptyArtifacts := filepath.Join(dataDir, "empty_artifacts")
	_ = mustExecCommand(t, "folders", dataDir, "--regexp", `empty`, "--remove")
	if _, err := os.Stat(emptyArtifacts); !os.IsNotExist(err) {
		t.Fatalf("empty_artifacts folder should have been removed")
	}

	// =========================================================================
	// Phase 2: ClearPath (Pfad-Sanitisierung)
	// =========================================================================
	t.Log("=== Phase 2.1: ClearPath im Dry-Run Modus ===")
	puaBefore := filepath.Join(dataDir, "dirty_dir", "pua\uE001file.xml")
	dashBefore := filepath.Join(dataDir, "dirty_dir", "em\u2014dash.txt")
	dotBefore := filepath.Join(dataDir, "dirty_dir", "..prefix.json")
	spaceBefore := filepath.Join(dataDir, "dirty_dir", "multi   spaces.txt")

	if _, err := os.Stat(puaBefore); os.IsNotExist(err) {
		t.Fatalf("expected pua file to exist before clearpath: %s", puaBefore)
	}

	// Dry-run: should not rename files on disk
	_ = mustExecCommand(t, "clearpath", dataDir, "--auto")
	if _, err := os.Stat(puaBefore); os.IsNotExist(err) {
		t.Fatalf("dry-run should not have renamed file %s", puaBefore)
	}
	if _, err := os.Stat(dashBefore); os.IsNotExist(err) {
		t.Fatalf("dry-run should not have renamed file %s", dashBefore)
	}

	t.Log("=== Phase 2.2: ClearPath mit --rename (echte Umbenennung) ===")
	_ = mustExecCommand(t, "clearpath", dataDir, "--auto", "--rename")

	// PUA file should now be sanitized to puafile.xml
	if _, err := os.Stat(puaBefore); !os.IsNotExist(err) {
		t.Fatalf("sanitized PUA file should no longer exist under old name %s", puaBefore)
	}
	sanitizedXml := filepath.Join(dataDir, "dirty_dir", "puafile.xml")
	if _, err := os.Stat(sanitizedXml); os.IsNotExist(err) {
		t.Fatalf("expected sanitized file %s to exist", sanitizedXml)
	}

	// Dash file should now be normalized to em-dash.txt
	if _, err := os.Stat(dashBefore); !os.IsNotExist(err) {
		t.Fatalf("sanitized dash file should no longer exist under old name %s", dashBefore)
	}
	sanitizedDash := filepath.Join(dataDir, "dirty_dir", "em-dash.txt")
	if _, err := os.Stat(sanitizedDash); os.IsNotExist(err) {
		t.Fatalf("expected sanitized dash file %s to exist", sanitizedDash)
	}

	// Double dot file should now be normalized to _.prefix.json
	if _, err := os.Stat(dotBefore); !os.IsNotExist(err) {
		t.Fatalf("sanitized dot file should no longer exist under old name %s", dotBefore)
	}
	sanitizedDot := filepath.Join(dataDir, "dirty_dir", "_.prefix.json")
	if _, err := os.Stat(sanitizedDot); os.IsNotExist(err) {
		t.Fatalf("expected sanitized dot file %s to exist", sanitizedDot)
	}

	// Multi space file should now be collapsed to multi spaces.txt
	if _, err := os.Stat(spaceBefore); !os.IsNotExist(err) {
		t.Fatalf("sanitized space file should no longer exist under old name %s", spaceBefore)
	}
	sanitizedSpace := filepath.Join(dataDir, "dirty_dir", "multi spaces.txt")
	if _, err := os.Stat(sanitizedSpace); os.IsNotExist(err) {
		t.Fatalf("expected sanitized space file %s to exist", sanitizedSpace)
	}

	// =========================================================================
	// Phase 3: Indexierung und technische Metadatenextraktion (index)
	// =========================================================================
	t.Log("=== Phase 3: Vollständige Indexierung nach BadgerDB und Multi-Format-Exporte ===")
	indexCsv := filepath.Join(reportsDir, "index.csv")
	indexJsonl := filepath.Join(reportsDir, "index.jsonl")
	indexXlsx := filepath.Join(reportsDir, "index.xlsx")

	_ = mustExecCommand(t, "index", dataDir,
		"--database", dbDir,
		"--csv", indexCsv,
		"--jsonl", indexJsonl,
		"--xlsx", indexXlsx,
		"--concurrent", "2",
	)

	// Validate exported JSONL records
	indexedRecords := readJSONLRecords(t, indexJsonl)
	if len(indexedRecords) == 0 {
		t.Fatalf("expected indexed records in JSONL, got 0")
	}
	t.Logf("Indexed %d files into database and reports", len(indexedRecords))

	// Verify CSV export
	csvRows := readCSVRecords(t, indexCsv)
	if len(csvRows) != len(indexedRecords)+1 { // +1 for header
		t.Fatalf("expected %d CSV rows (including header), got %d", len(indexedRecords)+1, len(csvRows))
	}

	// Verify XLSX export can be opened
	xlFile, err := xlsx.OpenFile(indexXlsx)
	if err != nil {
		t.Fatalf("failed to open generated index XLSX: %v", err)
	}
	if len(xlFile.Sheets) == 0 || xlFile.Sheets[0].MaxRow < 2 {
		t.Fatalf("XLSX file has insufficient rows or sheets")
	}

	// Verify duplicates and metadata detection
	var dupCount int
	var emptyCount int
	var xmlCount int
	for _, rec := range indexedRecords {
		if rec.Duplicate {
			dupCount++
		}
		if rec.Size == 0 {
			emptyCount++
		}
		if rec.Indexer != nil && (strings.Contains(rec.Indexer.Mimetype, "xml") || rec.Indexer.Type == "xml") {
			xmlCount++
		}
		// Every non-empty file should have a SHA512 checksum
		if rec.Size > 0 && (rec.Indexer == nil || len(rec.Indexer.Checksum) == 0) {
			t.Fatalf("record %s is missing checksums", rec.Path)
		}
	}

	if dupCount < 2 {
		t.Fatalf("expected at least 2 duplicate records detected, got %d", dupCount)
	}
	if emptyCount != 1 {
		t.Fatalf("expected exactly 1 empty file detected, got %d", emptyCount)
	}
	if xmlCount < 2 {
		t.Fatalf("expected at least 2 XML files indexed, got %d", xmlCount)
	}
	t.Logf("Verification: Found %d duplicates, %d empty files, %d XML documents", dupCount, emptyCount, xmlCount)

	// =========================================================================
	// Phase 4: Index-Abfragen und Filterungen (index list)
	// =========================================================================
	t.Log("=== Phase 4.1: index list - Duplikate filtern ===")
	dupListJsonl := filepath.Join(reportsDir, "duplicates.jsonl")
	_ = mustExecCommand(t, "index", "list",
		"--database", dbDir,
		"--duplicates",
		"--jsonl", dupListJsonl,
	)
	dupRecords := readJSONLRecords(t, dupListJsonl)
	if len(dupRecords) != dupCount {
		t.Fatalf("expected %d duplicate records from index list, got %d", dupCount, len(dupRecords))
	}
	for _, rec := range dupRecords {
		if !rec.Duplicate {
			t.Fatalf("non-duplicate returned in duplicates query: %s", rec.Path)
		}
	}

	t.Log("=== Phase 4.2: index list - Leere Dateien filtern ===")
	emptyListJsonl := filepath.Join(reportsDir, "empty.jsonl")
	_ = mustExecCommand(t, "index", "list",
		"--database", dbDir,
		"--empty",
		"--jsonl", emptyListJsonl,
	)
	emptyRecords := readJSONLRecords(t, emptyListJsonl)
	if len(emptyRecords) != 1 || emptyRecords[0].Size != 0 {
		t.Fatalf("expected 1 empty record from index list, got %d", len(emptyRecords))
	}

	t.Log("=== Phase 4.3: index list - Regex Filter auf XML Dateien ===")
	xmlListJsonl := filepath.Join(reportsDir, "xml_only.jsonl")
	_ = mustExecCommand(t, "index", "list",
		"--database", dbDir,
		"--regexp", `\.xml$`,
		"--jsonl", xmlListJsonl,
	)
	xmlRecords := readJSONLRecords(t, xmlListJsonl)
	if len(xmlRecords) < 2 {
		t.Fatalf("expected at least 2 XML records from regex filter, got %d", len(xmlRecords))
	}

	// =========================================================================
	// Phase 5: Aggregierte Statistiken (index folders, index mime, index pronom)
	// =========================================================================
	t.Log("=== Phase 5.1: index folders - Verzeichnisstatistiken ===")
	foldersCsv := filepath.Join(reportsDir, "folders.csv")
	foldersJsonl := filepath.Join(reportsDir, "folders.jsonl")
	foldersXlsx := filepath.Join(reportsDir, "folders.xlsx")
	_ = mustExecCommand(t, "index", "folders",
		"--database", dbDir,
		"--csv", foldersCsv,
		"--jsonl", foldersJsonl,
		"--xlsx", foldersXlsx,
	)
	foldersRows := readCSVRecords(t, foldersCsv)
	if len(foldersRows) < 2 {
		t.Fatalf("expected folder statistics in CSV")
	}

	t.Log("=== Phase 5.2: index mime - MIME-Type Statistiken ===")
	mimeCsv := filepath.Join(reportsDir, "mime.csv")
	mimeJsonl := filepath.Join(reportsDir, "mime.jsonl")
	mimeXlsx := filepath.Join(reportsDir, "mime.xlsx")
	_ = mustExecCommand(t, "index", "mime",
		"--database", dbDir,
		"--csv", mimeCsv,
		"--jsonl", mimeJsonl,
		"--xlsx", mimeXlsx,
	)
	mimeRows := readCSVRecords(t, mimeCsv)
	if len(mimeRows) < 2 {
		t.Fatalf("expected MIME statistics in CSV")
	}

	t.Log("=== Phase 5.3: index pronom - PRONOM Format Statistiken ===")
	pronomCsv := filepath.Join(reportsDir, "pronom.csv")
	pronomJsonl := filepath.Join(reportsDir, "pronom.jsonl")
	pronomXlsx := filepath.Join(reportsDir, "pronom.xlsx")
	_ = mustExecCommand(t, "index", "pronom",
		"--database", dbDir,
		"--csv", pronomCsv,
		"--jsonl", pronomJsonl,
		"--xlsx", pronomXlsx,
	)
	pronomRows := readCSVRecords(t, pronomCsv)
	if len(pronomRows) < 2 {
		t.Fatalf("expected PRONOM statistics in CSV")
	}

	// =========================================================================
	// Phase 6: Bereinigung über den Index (index list --remove)
	// =========================================================================
	t.Log("=== Phase 6.1: index list mit --remove für leere Dateien ===")
	emptyDocPath := filepath.Join(dataDir, "documents", "empty_file.txt")
	if _, err := os.Stat(emptyDocPath); os.IsNotExist(err) {
		t.Fatalf("expected empty_file.txt to exist before removal")
	}

	_ = mustExecCommand(t, "index", "list", dataDir,
		"--database", dbDir,
		"--empty",
		"--remove",
	)
	if _, err := os.Stat(emptyDocPath); !os.IsNotExist(err) {
		t.Fatalf("empty_file.txt should have been deleted from disk")
	}

	t.Log("=== Phase 6.2: index list mit --remove für Duplikate ===")
	articlePaths := []string{
		filepath.Join(dataDir, "documents", "article.txt"),
		filepath.Join(dataDir, "duplicates_folder_a", "article_duplicate_1.txt"),
		filepath.Join(dataDir, "duplicates_folder_b", "article_duplicate_2.txt"),
	}
	catalogPaths := []string{
		filepath.Join(dataDir, "documents", "catalog.xml"),
		filepath.Join(dataDir, "duplicates_folder_b", "catalog_duplicate.xml"),
	}

	// Verify all duplicate copies exist before removal
	for _, p := range append(articlePaths, catalogPaths...) {
		if _, err := os.Stat(p); os.IsNotExist(err) {
			t.Fatalf("expected file %s to exist before duplicate removal", p)
		}
	}

	_ = mustExecCommand(t, "index", "list", dataDir,
		"--database", dbDir,
		"--duplicates",
		"--remove",
	)

	// Exactly 1 copy of the article group should remain on disk
	var remainingArticles int
	for _, p := range articlePaths {
		if _, err := os.Stat(p); err == nil {
			remainingArticles++
		}
	}
	if remainingArticles != 1 {
		t.Fatalf("expected exactly 1 article copy to remain after duplicate removal, got %d", remainingArticles)
	}

	// Exactly 1 copy of the catalog group should remain on disk
	var remainingCatalogs int
	for _, p := range catalogPaths {
		if _, err := os.Stat(p); err == nil {
			remainingCatalogs++
		}
	}
	if remainingCatalogs != 1 {
		t.Fatalf("expected exactly 1 catalog copy to remain after duplicate removal, got %d", remainingCatalogs)
	}

	// Unique files must still exist
	uniqueFiles := []string{
		filepath.Join(dataDir, "documents", "config.json"),
		filepath.Join(dataDir, "dirty_dir", "puafile.xml"),
		filepath.Join(dataDir, "dirty_dir", "em-dash.txt"),
		filepath.Join(dataDir, "dirty_dir", "_.prefix.json"),
		filepath.Join(dataDir, "dirty_dir", "multi spaces.txt"),
	}
	for _, p := range uniqueFiles {
		if _, err := os.Stat(p); os.IsNotExist(err) {
			t.Fatalf("unique file %s should NOT have been deleted", p)
		}
	}

	t.Log("=== Integration Test erfolgreich abgeschlossen! ===")
}
