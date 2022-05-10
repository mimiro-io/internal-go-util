package graphql

import (
	"fmt"
	"github.com/99designs/gqlgen/graphql"
	"github.com/rs/xid"
	"io"
	"strconv"
)

func MarshalXID(u xid.ID) graphql.Marshaler {
	return graphql.WriterFunc(
		func(w io.Writer) {
			io.WriteString(w, strconv.Quote(u.String()))
		},
	)
}

func UnmarshalXID(v interface{}) (xid.ID, error) {
	switch v := v.(type) {
	case string:
		uid, err := xid.FromString(v)
		if err != nil {
			return xid.NilID(), fmt.Errorf("%T is not an UUID: %w", v, err)
		}
		return uid, nil
	default:
		return xid.NilID(), fmt.Errorf("%T is not an UUID", v)
	}
}
