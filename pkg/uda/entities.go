package uda

import (
	"fmt"
	"strings"
)

type Entity struct {
	ID         string         `json:"id"`
	IsDeleted  bool           `json:"deleted"`
	References map[string]any `json:"refs"`
	Properties map[string]any `json:"props"`
}

// NewEntity Create a new entity
func NewEntity() *Entity {
	e := Entity{}
	e.Properties = make(map[string]any)
	e.References = make(map[string]any)
	return &e
}

type Context struct {
	ID              string            `json:"id"`
	Namespaces      map[string]string `json:"namespaces"`
	NamespaceLookup map[string]string `json:"omit"`
	RDFs            map[string]string `json:"omit"`
}

// StripPrefixes will strip the namespace prefix from any property, making it
// easier to work with
func (e *Entity) StripPrefixes() map[string]any {
	var properties = make(map[string]any)
	// dechunk the fields
	for k, v := range e.Properties {
		properties[dechunk(k)] = v
	}
	return properties
}

// dechunk will remove the first chunk of the string
func dechunk(s string) string {
	v := s
	if strings.Contains(v, ":") {
		return strings.Split(v, ":")[1]
	}
	return s
}

// ToURI will expand a ref into a full uri, replacing the curie with the full uri
func ToURI(context *Context, ref string) string {
	ns, val, _ := strings.Cut(ref, ":")
	l := context.Namespaces[ns]
	if l == "" { // probably just a regular ":" in the value, leave it alone
		return ref
	}

	return fmt.Sprintf("%s%s", l, val)
}
