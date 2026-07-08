package couch

import (
	"net/url"
)

// DefaultSeparator determines the character(s) to use to separate
// a prefix from the database name. Underscore is the default
// to be consistent with SQL table naming and JSON attributes.
const DefaultSeparator = "_"

// Config is used to define the connection details to a database.
type Config struct {
	Scheme    string `json:"scheme"`
	Host      string `json:"host"`
	Port      string `json:"port"`
	Username  string `json:"username"`
	Password  string `json:"password"`
	Prefix    string `json:"prefix"`
	Separator string `json:"separator"`
}

// NewConfig generates a new configuration instance and requires a prefix
// so that we have a nice namespace before all database names.
func NewConfig(prefix string) *Config {
	c := &Config{
		Scheme: "http",
		Host:   "couchdb", // assume we're in Docker
		Port:   "5984",
		Prefix: prefix,
	}
	return c
}

func (c *Config) baseURL() string {
	u := &url.URL{
		Scheme: c.Scheme,
		Host:   c.Host + ":" + c.Port,
	}
	if c.Username != "" {
		u.User = url.UserPassword(c.Username, c.Password)
	}
	return u.String()
}

// db provides a DB name including the configured prefix.
func (c *Config) db(name string) string {
	if name == "" {
		name = c.Prefix
	} else {
		name = c.Prefix + c.separator() + name
	}
	return name
}

func (c *Config) separator() string {
	if c.Separator != "" {
		return c.Separator
	}
	return DefaultSeparator
}
