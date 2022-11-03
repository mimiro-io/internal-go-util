package uda

import (
	"github.com/franela/goblin"
	"testing"
)

func TestEntity_StripProps(t *testing.T) {
	g := goblin.Goblin(t)

	var entity Entity
	g.Describe("Entity massage", func() {
		g.Before(func() {
			entity = Entity{
				"ns3:1",
				true,
				make(map[string]interface{}),
				make(map[string]interface{}),
			}
			entity.Properties["ns3:Product_Id"] = 679
		})
		g.It("Should strip namespace references from column names", func() {
			s := entity.StripPrefixes()
			_, ok := s["Product_Id"]
			g.Assert(ok).IsTrue()
		})
	})
}
