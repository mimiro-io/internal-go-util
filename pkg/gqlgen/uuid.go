package gqlgen

import (
	"fmt"
	"io"
	"strconv"

	"github.com/99designs/gqlgen/graphql"
	"github.com/gofrs/uuid"
)

// MarshalUUID returns the string form of uuid, xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
func MarshalUUID(u uuid.UUID) graphql.Marshaler {

	return graphql.WriterFunc(
		func(w io.Writer) {
			io.WriteString(w, strconv.Quote(u.String()))
		},
	)
}

func UnmarshalUUID(v interface{}) (uuid.UUID, error) {
	switch v := v.(type) {
	case string:
		uid, err := uuid.FromString(v)
		if err != nil {
			return uuid.Nil, fmt.Errorf("%T is not an UUID: %w", v, err)
		}
		return uid, nil
	default:
		return uuid.Nil, fmt.Errorf("%T is not an UUID", v)
	}
}
