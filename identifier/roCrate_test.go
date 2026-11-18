package identifier

import (
	"encoding/json/v2"
	"testing"
)

func TestRoCrateJSON(t *testing.T) {
	crateJSON := `{
		"@context": "https://w3id.org/ro/crate/1.1/context",
		"@graph": [
			{
				"@id": "ro-crate-metadata.json",
				"@type": "CreativeWork",
				"about": {"@id": "."}
			},
			{
				"@id": ".",
				"@type": ["Dataset", "RepositoryCollection"],
				"name": "Root Dataset",
				"description": "A sample dataset for testing",
				"hasPart": [
					{"@id": "subfolder/"},
					{"@id": "document.pdf"}
				],
				"customProp": "customValue"
			}
		]
	}`

	var crate RoCrate
	if err := json.Unmarshal([]byte(crateJSON), &crate); err != nil {
		t.Fatalf("failed to unmarshal RoCrate: %v", err)
	}

	root := crate.GetRoot()
	if root == nil {
		t.Fatal("expected root dataset not to be nil")
	}
	if root.ID != "." {
		t.Errorf("expected root ID '.', got %q", root.ID)
	}
	if root.Name != "Root Dataset" {
		t.Errorf("expected root name 'Root Dataset', got %q", root.Name)
	}
	if len(root.Type) != 2 || root.Type[0] != "Dataset" || root.Type[1] != "RepositoryCollection" {
		t.Errorf("expected types [Dataset, RepositoryCollection], got %v", root.Type)
	}
	if len(root.HasPart) != 2 {
		t.Errorf("expected 2 parts in root, got %d", len(root.HasPart))
	}
	if _, ok := root.Extra["customProp"]; !ok {
		t.Error("expected extra property 'customProp' to be preserved in Extra")
	}

	// Test marshaling
	marshaled, err := json.Marshal(&crate)
	if err != nil {
		t.Fatalf("failed to marshal RoCrate: %v", err)
	}

	var crateRoundTrip RoCrate
	if err := json.Unmarshal(marshaled, &crateRoundTrip); err != nil {
		t.Fatalf("failed to unmarshal round-tripped RoCrate: %v", err)
	}

	rootRT := crateRoundTrip.GetRoot()
	if rootRT == nil || rootRT.Name != "Root Dataset" {
		t.Errorf("round-trip failed to retain root dataset correctly")
	}
}

func TestStringOrList(t *testing.T) {
	var single StringOrList
	if err := json.Unmarshal([]byte(`"Dataset"`), &single); err != nil {
		t.Fatalf("unmarshal single string failed: %v", err)
	}
	if len(single) != 1 || single[0] != "Dataset" {
		t.Errorf("expected [Dataset], got %v", single)
	}

	var multiple StringOrList
	if err := json.Unmarshal([]byte(`["Dataset", "Collection"]`), &multiple); err != nil {
		t.Fatalf("unmarshal string array failed: %v", err)
	}
	if len(multiple) != 2 || multiple[0] != "Dataset" || multiple[1] != "Collection" {
		t.Errorf("expected [Dataset, Collection], got %v", multiple)
	}
}
