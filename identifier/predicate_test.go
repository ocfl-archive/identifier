package identifier

import (
	"regexp"
	"testing"

	"github.com/ocfl-archive/indexer/v3/pkg/indexer"
)

func TestPredicateAndOr(t *testing.T) {
	isEven := func(n *int) bool { return *n%2 == 0 }
	isPositive := func(n *int) bool { return *n > 0 }

	both := And(isEven, isPositive)
	either := Or(isEven, isPositive)

	val4 := 4
	valNeg2 := -2
	val3 := 3
	valNeg3 := -3

	if !both(&val4) {
		t.Errorf("expected 4 to match both (even and positive)")
	}
	if both(&valNeg2) {
		t.Errorf("expected -2 to NOT match both")
	}
	if both(&val3) {
		t.Errorf("expected 3 to NOT match both")
	}

	if !either(&valNeg2) {
		t.Errorf("expected -2 to match either (even)")
	}
	if !either(&val3) {
		t.Errorf("expected 3 to match either (positive)")
	}
	if either(&valNeg3) {
		t.Errorf("expected -3 to NOT match either")
	}
}

func TestBuildFileFilter(t *testing.T) {
	re := regexp.MustCompile(`\.pdf$`)

	filterAll := BuildFileFilter(false, false, nil)
	filterEmpty := BuildFileFilter(true, false, nil)
	filterDupes := BuildFileFilter(false, true, nil)
	filterRegex := BuildFileFilter(false, false, re)

	emptyFile := &FileData{Basename: "empty.txt", Size: 0, Indexer: &indexer.ResultV2{}}
	normalPdf := &FileData{Basename: "document.pdf", Size: 100, Indexer: &indexer.ResultV2{}}
	normalTxt := &FileData{Basename: "notes.txt", Size: 50, Indexer: &indexer.ResultV2{}}
	duplicateFile := &FileData{Basename: "copy.txt", Size: 50, Duplicate: true, Indexer: &indexer.ResultV2{}}

	if !filterAll(normalTxt) {
		t.Errorf("filterAll should match standard file")
	}

	if !filterEmpty(emptyFile) {
		t.Errorf("filterEmpty should match empty file")
	}
	if filterEmpty(normalTxt) {
		t.Errorf("filterEmpty should not match non-empty file")
	}

	if !filterDupes(duplicateFile) {
		t.Errorf("filterDupes should match duplicate file")
	}
	if filterDupes(normalTxt) {
		t.Errorf("filterDupes should not match non-duplicate file")
	}

	if !filterRegex(normalPdf) {
		t.Errorf("filterRegex should match .pdf file")
	}
	if filterRegex(normalTxt) {
		t.Errorf("filterRegex should not match .txt file")
	}
}
