package uda

import (
	"github.com/bcicen/jstream"
	"io"
	"strings"
)

//Parse will give a full slice of entities in one go, if oyu only have a few, this is ok,
// but if you have millions then this will blow up, and you should use the ParseStream instead
func Parse(reader io.Reader) ([]any, error) {
	entities := make([]any, 0)
	isFirst := true

	err := ParseStream(reader, func(value *jstream.MetaValue) error {
		if isFirst {
			entities = append(entities, AsContext(value))
			isFirst = false
		} else {
			entities = append(entities, AsEntity(value))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entities, nil
}

//ParseStream The following functions should probably be in separat file
func ParseStream(reader io.Reader, emitEntity func(value *jstream.MetaValue) error) error {
	decoder := jstream.NewDecoder(reader, 1) //Reads json

	for mv := range decoder.Stream() { //Stream begins decoding from reader and returns a streaming MetaValue channel for JSON values at the configured emitDepth.
		err := emitEntity(mv)
		if err != nil {
			return err
		}
	}

	return nil
}

func AsContext(value *jstream.MetaValue) *Context {
	raw := value.Value.(map[string]interface{})
	ctx := &Context{
		ID:              raw["id"].(string),
		Namespaces:      map[string]string{},
		NamespaceLookup: map[string]string{},
		RDFs:            map[string]string{},
	}
	namespaces, ok := raw["namespaces"]
	if ok {
		ns := make(map[string]string)
		for k, v := range namespaces.(map[string]any) {
			ns[k] = v.(string)
		}

		ctx.Namespaces = ns

		// make a lookup
		lookup := make(map[string]string)
		rdfs := make(map[string]string)
		for k, v := range ctx.Namespaces {
			lookup[v] = k
			rdfs[k] = namespace(v)
		}
		ctx.NamespaceLookup = lookup
		ctx.RDFs = rdfs
	}
	return ctx
}

func AsEntity(value *jstream.MetaValue) *Entity {
	entity := NewEntity()
	raw := value.Value.(map[string]interface{})

	entity.ID = raw["id"].(string)
	deleted, ok := raw["deleted"]
	if ok {
		entity.IsDeleted = deleted.(bool)
	}

	props, ok := raw["props"]
	if ok {
		entity.Properties = props.(map[string]any)
	}
	refs, ok := raw["refs"]
	if ok {
		entity.References = refs.(map[string]any)
	}

	return entity
}

func namespace(ns string) string {
	// clean up a bit
	clean := strings.ReplaceAll(ns, "/", " ")
	clean = strings.TrimSpace(clean)
	parts := strings.Fields(clean)
	return parts[len(parts)-1]
}
