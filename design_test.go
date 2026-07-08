package couch_test

import (
	"testing"

	"github.com/invopop/couch"
	"github.com/stretchr/testify/assert"
)

func TestChecksumIgnoresNilView(t *testing.T) {
	d := couch.NewDesign("test")
	d.SetView("ok", &couch.View{Map: "function(doc) {}"})
	d.Views["broken"] = nil // a nil entry must not panic the checksum
	assert.NotPanics(t, func() { _ = d.Checksum() })
}

func TestDesignInstantiation(t *testing.T) {
	design := couch.NewDesign("test")
	if design.ID != "_design/test" {
		t.Errorf("Unexpected doc ID: %s", design.ID)
	}
	if design.Language != "javascript" {
		t.Errorf("Unexpected language: %s", design.Language)
	}
	if design.Views == nil {
		t.Error("View map is not initialized!")
	}
}

func TestChecksum(t *testing.T) {
	design := couch.NewDesign("test")
	design.SetView("by_created_at", &couch.View{
		Map:    "function(d) { if (d['created_at']) { emit(d['created_at'], 1); } }",
		Reduce: "_sum",
	})
	cs := design.Checksum()
	if want := "2e1c80b5f2eb78fec2396a11dce712648710d300d500c9a392034a21d90bbbcd"; want != cs {
		t.Errorf("unexpected checksum: %s", cs)
	}
	design.Views["by_created_at"].Reduce = "_stats"
	if design.Checksum() == cs {
		t.Error("Checksums match when they should differ!")
	}
}
