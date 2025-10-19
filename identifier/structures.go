package identifier

import (
	"fmt"

	"github.com/ocfl-archive/indexer/v3/pkg/indexer"
)

type FileData struct {
	Path      string            `json:"path,omitempty"`
	Folder    string            `json:"folder,omitempty"`
	Basename  string            `json:"basename,omitempty"`
	Size      int64             `json:"size,omitempty"`
	Duplicate bool              `json:"duplicate,omitempty"`
	LastMod   int64             `json:"lastmod,omitempty"`
	Indexer   *indexer.ResultV2 `json:"indexer,omitempty"`
	LastSeen  int64             `json:"lastseen,omitempty"`
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
