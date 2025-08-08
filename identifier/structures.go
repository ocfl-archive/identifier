package identifier

import (
	"fmt"

	"github.com/ocfl-archive/indexer/v3/pkg/indexer"
)

type FileData struct {
	Path      string            `json:"path"`
	Folder    string            `json:"folder"`
	Basename  string            `json:"basename"`
	Size      int64             `json:"size"`
	Duplicate bool              `json:"duplicate"`
	LastMod   int64             `json:"lastmod"`
	Indexer   *indexer.ResultV2 `json:"indexer"`
	LastSeen  int64             `json:"lastseen"`
}

type AIPerson struct {
	Name string
	Role string
}

func (p AIPerson) String() string {
	result := p.Name
	if p.Role != "" {
		result += fmt.Sprintf(" [%s]", p.Role)
	}
	return result
}

type AIResultStruct struct {
	Folder       string
	Title        string
	Description  string
	Place        string
	Date         string
	Tags         []string
	Persons      []AIPerson
	Institutions []string
}
