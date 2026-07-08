package couch

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigBaseURL(t *testing.T) {
	c := NewConfig("app")
	assert.Equal(t, "http://couchdb:5984", c.baseURL(), "defaults")

	c.Scheme = "https"
	c.Host = "db.example"
	c.Port = "6984"
	assert.Equal(t, "https://db.example:6984", c.baseURL())

	c.Username = "admin"
	c.Password = "s3cr3t"
	assert.Equal(t, "https://admin:s3cr3t@db.example:6984", c.baseURL(), "with credentials")
}

func TestConfigDBNaming(t *testing.T) {
	c := NewConfig("app")
	assert.Equal(t, "app_widgets", c.db("widgets"), "prefix + separator + name")
	assert.Equal(t, "app", c.db(""), "empty name resolves to the prefix alone")

	c.Separator = "-"
	assert.Equal(t, "app-widgets", c.db("widgets"), "custom separator")
}
