package identifier

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"

	"emperror.dev/errors"
)

// StringOrList is a helper type for JSON-LD properties that can appear as either a single string
// or an array of strings (e.g., "@type": "Dataset" vs "@type": ["Dataset", "Collection"]).
type StringOrList []string

// UnmarshalJSON unmarshals either a JSON string or a JSON array of strings into StringOrList.
func (s *StringOrList) UnmarshalJSON(data []byte) error {
	if data[0] == '[' {
		var list []string
		if err := json.Unmarshal(data, &list); err != nil {
			return err
		}
		*s = list
		return nil
	}
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	*s = []string{str}
	return nil
}

// RoCrateGraph represents an ordered list of RO-Crate entity nodes or entity references in an RO-Crate @graph.
type RoCrateGraph []*RoCrateGraphElement

// UnmarshalJSON unmarshals either a single object with "@id" or an array of graph elements.
func (r *RoCrateGraph) UnmarshalJSON(data []byte) error {
	var e = map[string]string{}
	if err := json.Unmarshal(data, &e); err == nil {
		if _, ok := e["@id"]; !ok {
			return errors.New("missing @id")
		}
		*r = []*RoCrateGraphElement{{ID: e["@id"]}}
		return nil
	}
	var list = []*RoCrateGraphElement{}
	if err := json.Unmarshal(data, &list); err != nil {
		return errors.Wrap(err, "cannot unmarshal RoCrateGraph")
	}
	*r = list
	return nil
}

// RoCrateGraphElement represents a single entity in the RO-Crate JSON-LD graph
// conforming to the RO-Crate 1.1 specification (e.g. CreativeWork, Dataset, File).
type RoCrateGraphElement struct {
	// ID is the URI or relative path identifier for this entity ("@id").
	ID string `json:"@id"`

	// Type represents the Schema.org / RO-Crate types of the entity ("@type").
	Type StringOrList `json:"@type,omitempty"`

	// HasPart lists child entities or parts belonging to this dataset or folder.
	HasPart RoCrateGraph `json:"hasPart,omitempty"`

	// Name is the human-readable display name or title of the entity.
	Name string `json:"name,omitempty"`

	// Description is the text description or contextual summary of the entity.
	Description string `json:"description,omitempty"`

	// About references entities that this descriptor describes (e.g. the root dataset ".").
	About RoCrateGraph `json:"about,omitempty"`

	// Extra preserves unknown or extension JSON-LD properties during unmarshaling and marshaling.
	Extra map[string]jsontext.Value `json:"-"`
}

// String returns a human-readable identifier with name if available.
func (r *RoCrateGraphElement) String() string {
	if r.Name != "" {
		return fmt.Sprintf("%s (%s)", r.Name, r.ID)
	}
	return r.ID
}

// MarshalJSON custom-marshals the graph element, combining core fields and extra preserved properties.
func (r *RoCrateGraphElement) MarshalJSON() ([]byte, error) {
	theMap := map[string]any{
		"@id": r.ID,
	}
	if len(r.Type) > 0 {
		theMap["@type"] = r.Type
	}
	if len(r.HasPart) > 0 {
		theMap["hasPart"] = r.HasPart
	}
	if r.Name != "" {
		theMap["name"] = r.Name
	}
	if r.Description != "" {
		theMap["description"] = r.Description
	}
	if len(r.About) > 0 {
		theMap["about"] = r.About
	}
	for key, value := range r.Extra {
		theMap[key] = value
	}

	data, err := json.Marshal(theMap)
	if err != nil {
		return nil, errors.Wrap(err, "cannot marshal RoCrateGraphElement")
	}
	return data, nil
}

// UnmarshalJSON custom-unmarshals the graph element, capturing core fields and storing extra fields in Extra.
func (r *RoCrateGraphElement) UnmarshalJSON(data []byte) error {
	type Alias RoCrateGraphElement
	a := (*Alias)(r)
	if err := json.Unmarshal(data, a); err != nil {
		return errors.Wrap(err, "cannot unmarshal RoCrateGraphElement")
	}
	*r = (RoCrateGraphElement)(*a)
	if r.Extra == nil {
		r.Extra = map[string]jsontext.Value{}
		d := map[string]jsontext.Value{}
		if err := json.Unmarshal(data, &d); err != nil {
			return errors.Wrap(err, "cannot unmarshal RoCrateGraphElement extra")
		}
		for key, value := range d {
			switch key {
			case "@id", "@type", "hasPart", "name", "description", "about":
				continue
			}
			r.Extra[key] = value
		}
	}
	return nil
}

// AddPart appends a reference to child element ID to HasPart if not already present.
func (r *RoCrateGraphElement) AddPart(id string, b bool) {
	if r.HasPart == nil {
		r.HasPart = RoCrateGraph{}
	}
	for _, e := range r.HasPart {
		if e.ID == id {
			if b {
				e.ID = id
			}
			return
		}
	}
	r.HasPart = append(r.HasPart, &RoCrateGraphElement{ID: id})
}

// RoCrate represents a complete RO-Crate metadata document (ro-crate-metadata.json).
type RoCrate struct {
	// Context is the JSON-LD @context definition (usually "https://w3id.org/ro/crate/1.1/context").
	Context jsontext.Value `json:"@context"`

	// Graph contains all entities and contextual relationships in the RO-Crate.
	Graph RoCrateGraph `json:"@graph"`
}

// Get finds an entity by its @id URI or relative path.
func (r *RoCrate) Get(id string) *RoCrateGraphElement {
	for _, e := range r.Graph {
		if e.ID == id {
			return e
		}
	}
	return nil
}

// GetAbout returns entities referenced by the metadata descriptor's "about" field.
func (r *RoCrate) GetAbout() RoCrateGraph {
	var result = RoCrateGraph{}
	for _, e := range r.Graph {
		if e.ID == "ro-crate-metadata.json" {
			for _, a := range e.About {
				elem := r.Get(a.ID)
				if elem != nil {
					result = append(result, elem)
				}
			}
			break
		}
	}
	return result
}

// GetRoot retrieves the root Dataset entity of the RO-Crate (typically "@id": ".").
func (r *RoCrate) GetRoot() *RoCrateGraphElement {
	for _, e := range r.Graph {
		if e.ID == "ro-crate-metadata.json" {
			if len(e.About) == 0 {
				return nil
			}
			return r.Get(e.About[0].ID)
		}
	}
	return nil
}

// GetParts returns all part IDs attached to root entities in the crate.
func (r *RoCrate) GetParts() (ids []string) {
	ids = []string{}
	for _, a := range r.GetAbout() {
		for _, p := range a.HasPart {
			ids = append(ids, p.ID)
		}
	}
	return
}

// AddElement adds or updates an entity in the graph. If replace is true, existing entity with same ID is overwritten.
func (r *RoCrate) AddElement(elem *RoCrateGraphElement, replace bool) {
	for i, e := range r.Graph {
		if e.ID == elem.ID {
			if replace {
				r.Graph[i] = elem
			}
			return
		}
	}
	r.Graph = append(r.Graph, elem)
	root := r.GetRoot()
	if root == nil {
		return
	}
	if root.HasPart == nil {
		root.HasPart = RoCrateGraph{}
	}
}
