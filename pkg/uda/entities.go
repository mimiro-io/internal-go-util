package uda

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
	RDFs            map[string]string
}
