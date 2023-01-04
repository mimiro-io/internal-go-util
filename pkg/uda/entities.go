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
	Recorded   string         `json:"recorded,omitempty"`
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
	NamespaceLookup map[string]string `json:"-"`
	RDFs            map[string]string `json:"-"`
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

// ExpandUris will expand ns ref into full uris for id and refs for a list of entities
func ExpandUris(entities []*Entity, context *Context) []*Entity {
	var expandedEntities []*Entity

	for _, entity := range entities {
		entity.ID = ToURI(context, entity.ID)
		newRefs := make(map[string]any)
		for refKey, refValue := range entity.References {
			if values, ok := refValue.([]any); ok {
				var newValues []string
				for _, val := range values {
					newValues = append(newValues, ToURI(context, val.(string)))
				}
				newRefs[refKey] = newValues
			} else {
				newRefs[refKey] = ToURI(context, refValue.(string))
			}
		}
		entity.References = newRefs
		expandedEntities = append(expandedEntities, entity)
	}
	return expandedEntities
}

func keyStripper(entity *Entity, keyType string) map[string]any {
	var singleMap = make(map[string]any)
	var keys map[string]any
	switch keyType {
	case "props":
		keys = entity.Properties
	case "refs":
		keys = entity.References
	}
	for k := range keys {
		key := k
		parts := strings.SplitAfter(k, ":")
		if len(parts) > 1 {
			key = parts[1]
		}
		singleMap[key] = keys[k]
	}

	return singleMap
}

// StripRefs will strip namespace prefix from entity reference keys
func StripRefs(entity *Entity) map[string]any {
	return keyStripper(entity, "refs")
}

// StripProps will strip namespace prefix from entity property keys
func StripProps(entity *Entity) map[string]any {
	return keyStripper(entity, "props")
}
