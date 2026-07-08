package couch

import (
	"fmt"

	"github.com/go-kivik/kivik/v4"
)

// Shardable defines what we expect from a document, entity, or model that
// we intend to persist to the database.
type Shardable interface {
	ShardValue() interface{}
}

// ShardRules provides the basic details we require to properly handle sharding
// of a type of object.
type ShardRules interface {
	// Template provides the base name into which the shard will be inserted.
	Template() string

	// List provides an array of acceptable shards
	List() []string

	// Key provides a usable string from a shardable value.
	Key(v interface{}) (string, error)
}

// Shards is a special implementation of sharding at the software level.
// The aim is to make it easier to manage a set of separate CouchDB databases
// each of which is used according to sharding details provided.
type Shards struct {
	names []string
	dbs   map[string]*kivik.DB
	rules ShardRules
}

// NewShards instantiates a new sharding wrapper. Databases cannot be assigned
// dynamically, a complete list of databases must be prepared.
func NewShards(client *Client, rules ShardRules) *Shards {
	s := new(Shards)
	s.names = rules.List()
	s.dbs = make(map[string]*kivik.DB)
	s.rules = rules
	for _, shard := range s.names {
		name := fmt.Sprintf(rules.Template(), shard)
		s.dbs[shard] = client.DB(name)
	}
	return s
}

// For determines which shard to use for the provided "Shardable" model.
func (s *Shards) For(m Shardable) (*kivik.DB, error) {
	name, err := s.rules.Key(m.ShardValue())
	if err != nil {
		return nil, fmt.Errorf("invalid shard key: %w", err)
	}
	db, ok := s.dbs[name]
	if !ok {
		return nil, fmt.Errorf("invalid shard: %v", name)
	}
	return db, nil
}

// Get provides the requested database instance, or nil if the name is
// invalid.
func (s *Shards) Get(name string) *kivik.DB {
	return s.dbs[name]
}

// Names provides the complete list of shard names in use.
func (s *Shards) Names() []string {
	return s.names
}

// List provides an array of database objects, in the original shard order.
func (s *Shards) List() []*kivik.DB {
	list := make([]*kivik.DB, len(s.names))
	for i, n := range s.names {
		list[i] = s.Get(n)
	}
	return list
}

// Map provides the map of names to databases to be used for sharding.
// This is especially useful for performing migrations but caution should be
// taken in any other scenario as order is not guaranteed!
func (s *Shards) Map() map[string]*kivik.DB {
	return s.dbs
}
