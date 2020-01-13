package store

import (
	"encoding/json"
	"time"
	"errors"
	// "fmt"
)

// commandType are commands that affect the state of the cluster, and must go through Raft.
type commandType int

const (
	execute        commandType = iota // Commands which modify the database.
	query                             // Commands which query the database.
	metadataSet                       // Commands which sets Store metadata
	metadataDelete                    // Commands which deletes Store metadata
	connect                           // Commands which create a database connection
	disconnect                        // Commands which disconnect from the database.
)

type command struct {
	Typ commandType     `json:"typ"`
	Sub interface {} 	`json:"sub,omitempty"`
}

type rawMessage []byte

// MarshalJSON returns m as the JSON encoding of m.
func (m rawMessage) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	return m, nil
}

// UnmarshalJSON sets m to a reference of data
func (m *rawMessage) UnmarshalJSON(data []byte) error {
	if m == nil {
		return errors.New("json.RawMessage: UnmarshalJSON on nil pointer")
	}
	// m = (*rawMessage)(&data)
	*m = append((*m)[0:0], data...)
	// fmt.Printf("\n%s\n", *m)
	// fmt.Printf("\n%s\n", *(*rawMessage)(&data))
	return nil
}

func (c *command) MarshalJSON() ([]byte, error){
	switch c.Typ{
		case execute, query: 
			return json.Marshal(&struct{
				Typ commandType     `json:"typ"`
				Sub *databaseSub 	`json:"sub,omitempty"`							
			}{
				Typ: c.Typ, 
				Sub: c.Sub.(*databaseSub),
			})
		case metadataSet:
			return json.Marshal(&struct{
				Typ commandType     `json:"typ"`
				Sub *metadataSetSub `json:"sub,omitempty"`							
			}{
				Typ: c.Typ, 
				Sub: c.Sub.(*metadataSetSub),
			})		
		case metadataDelete:
			return json.Marshal(&struct{
				Typ commandType     `json:"typ"`
				Sub *string 		`json:"sub,omitempty"`							
			}{
				Typ: c.Typ, 
				Sub: c.Sub.(*string),
			})

		case connect, disconnect:
			return json.Marshal(&struct{
				Typ commandType     `json:"typ"`
				Sub *connectionSub 		`json:"sub,omitempty"`							
			}{
				Typ: c.Typ, 
				Sub: c.Sub.(*connectionSub),
			})

		default:
			return nil, errors.New("UnmarshalJSON: unknown type")
	}
}

func (c *command) UnmarshalJSON(b []byte) error {
	// first, unmarshall into a map from string to json.RawMessage
	partialUnmarshal := new(struct{
		Typ *rawMessage 	`json:"typ"`
		Sub *rawMessage 	`json:"sub"`
	}) 
	if err := json.Unmarshal(b, &partialUnmarshal); err != nil {
		return err
	}

	if partialUnmarshal.Typ == nil {
		// return fmt.Errorf("command type not found. json is %s", b[:100])
		c.Typ = execute
	} else {
		if err := json.Unmarshal(*partialUnmarshal.Typ, &c.Typ); err != nil {
			return err
		}	
	}

	switch c.Typ{
		case execute, query: 
			c.Sub = new(databaseSub)
			subPtr := (c.Sub).(*databaseSub)
			return json.Unmarshal(*partialUnmarshal.Sub, subPtr)
		case metadataSet:
			c.Sub = new(metadataSetSub)
			subPtr := (c.Sub).(*metadataSetSub)
			return json.Unmarshal(*partialUnmarshal.Sub, subPtr)
		case metadataDelete: 
			c.Sub = new(string)
			subPtr := (c.Sub).(*string)
			return json.Unmarshal(*partialUnmarshal.Sub, subPtr)
		case connect, disconnect: 
			c.Sub = new(connectionSub)
			subPtr := (c.Sub).(*connectionSub)
			return json.Unmarshal(*partialUnmarshal.Sub, subPtr)

		default:
			return errors.New("UnmarshalJSON: unknown type")
	}

}

func newCommand(t commandType, d interface{}) (*command, error) {
	return &command{
		Typ: t,
		Sub: d,
	}, nil
}

func newMetadataSetCommand(id string, md map[string]string) (*command, error) {
	m := &metadataSetSub{
		RaftID: id,
		Data:   md,
	}
	return newCommand(metadataSet, m)
}

// databaseSub is a command sub which involves interaction with the database.
type databaseSub struct {
	ConnID  uint64   `json:"conn_id,omitempty"`
	Atomic  bool     `json:"atomic,omitempty"`
	Queries *[]string `json:"queries,omitempty"`
	Timings bool     `json:"timings,omitempty"`
}

type metadataSetSub struct {
	RaftID string            `json:"raft_id,omitempty"`
	Data   map[string]string `json:"data,omitempty"`
}

type connectionSub struct {
	ConnID      uint64        `json:"conn_id,omitempty"`
	IdleTimeout time.Duration `json:"idle_timeout,omitempty"`
	TxTimeout   time.Duration `json:"tx_timeout,omitempty"`
}
