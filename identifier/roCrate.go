package identifier

import (
	"emperror.dev/errors"
	"encoding/json"
	"fmt"
)

type StringOrList []string

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

type RoCrateGraph []*RoCrateGraphElement

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

type RoCrateGraphElement struct {
	ID          string                     `json:"@id"`
	Type        StringOrList               `json:"@type,omitempty"`
	HasPart     RoCrateGraph               `json:"hasPart,omitempty"`
	Name        string                     `json:"name,omitempty"`
	Description string                     `json:"description,omitempty"`
	About       RoCrateGraph               `json:"about,omitempty"`
	Extra       map[string]json.RawMessage `json:"-"`
}

func (r *RoCrateGraphElement) String() string {
	if r.Name != "" {
		return fmt.Sprintf("%s (%s)", r.Name, r.ID)
	}
	return r.ID
}

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

func (r *RoCrateGraphElement) UnmarshalJSON(data []byte) error {
	type Alias RoCrateGraphElement
	a := (*Alias)(r)
	if err := json.Unmarshal(data, a); err != nil {
		return errors.Wrap(err, "cannot unmarshal RoCrateGraphElement")
	}
	*r = (RoCrateGraphElement)(*a)
	if r.Extra == nil {
		r.Extra = map[string]json.RawMessage{}
		d := map[string]json.RawMessage{}
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

type RoCrate struct {
	Context json.RawMessage `json:"@context"`
	Graph   RoCrateGraph    `json:"@graph"`
}

func (r *RoCrate) Get(id string) *RoCrateGraphElement {
	for _, e := range r.Graph {
		if e.ID == id {
			return e
		}
	}
	return nil
}

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

func (r *RoCrate) GetParts() (ids []string) {
	ids = []string{}
	for _, a := range r.GetAbout() {
		for _, p := range a.HasPart {
			ids = append(ids, p.ID)
		}
	}
	return
}

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
	root.HasPart = append(root.HasPart, &RoCrateGraphElement{ID: elem.ID})
}
