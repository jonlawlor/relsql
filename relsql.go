// Package relsql implements a rel.Relation object that uses sql.DB.
// The name is nonstandard go because callers would always have to use a type
// alias to construct one otherwise.
package relcsv

import (
	"database/sql"
	"fmt"
	"github.com/jonlawlor/rel"
	"reflect"
)

// New creates a relation that reads from an sql table, with one tuple per row.
func New(db *sql.DB, tableName string, z interface{}, ckeystr [][]string) rel.Relation {
	if len(ckeystr) == 0 {
		return &sqlTable{db, tableName, colNames(z), z, rel.DefaultKeys(z), false, nil}
	}
	ckeys := rel.String2CandKeys(ckeystr)
	rel.OrderCandidateKeys(ckeys)
	return &sqlTable{db, tableName, colNames(z), z, rel.DefaultKeys(z), true, nil}
}

// colNames returns the names of the fields from a source tuple
func colNames(v interface{}) []string {
	e := reflect.TypeOf(v)
	n := e.NumField()
	names := make([]string, n)
	for i := 0; i < n; i++ {
		names[i] = e.Field(i).Name
	}
	return names
}

// sqlTable is an implementation of Relation using a csv.Reader
type sqlTable struct {
	// the *sql.DB connection, produced by an sql driver
	db *sql.DB

	// tablename is the name of the table in the database
	tableName string

	// the columns available in the table
	colNames []string

	// sqlzero and columns should always have the same number of fields

	// the type of the tuples returned by the relation
	zero interface{}

	// set of candidate keys
	cKeys rel.CandKeys

	// sourceDistinct indicates if the source sql was already distinct or if a
	// distinct has to be performed
	sourceDistinct bool

	// err holds the errors returned during query execution
	err error
}

func (r *sqlTable) TupleChan(t interface{}) chan<- struct{} {
	cancel := make(chan struct{})
	// reflect on the channel
	chv := reflect.ValueOf(t)
	err := rel.EnsureChan(chv.Type(), r.zero)
	if err != nil {
		r.err = err
		return cancel
	}
	if r.err != nil {
		chv.Close()
		return cancel
	}
	return cancel
}

// Zero returns the zero value of the relation (a blank tuple)
func (r *sqlTable) Zero() interface{} {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *sqlTable) CKeys() rel.CandKeys {
	return r.cKeys
}

// GoString returns a text representation of the Relation
func (r *sqlTable) GoString() string {
	return fmt.Sprintf("relsql.sqlTable{sql.DB, %s, %v, %v, %v, %v, %v}", r.tableName, r.colNames, r.zero, r.cKeys, r.sourceDistinct, r.err)
}

// String returns a text representation of the Relation
func (r *sqlTable) String() string {
	return "Relation(" + rel.HeadingString(r) + ")"
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
// this can be passed through to the sql server
func (r1 *sqlTable) Project(z2 interface{}) rel.Relation {

	// determine the location of the attributes that remain
	e1 := reflect.TypeOf(r1.zero)
	e2 := reflect.TypeOf(z2)

	if e1.AssignableTo(e2) {
		// nothing to do
		return r1
	}
	fMap := rel.FieldMap(e1, e2)

	// update the column names
	// it is important that they are in the same order as the new zero.
	colNames2 := colNames(z2)

	// update the candidate keys
	cKeys := rel.SubsetCandidateKeys(r1.cKeys, rel.Heading(r1), fMap)
	sourceDistinct := true
	// every relation except dee and dum have at least one candidate key
	if len(cKeys) == 0 {
		cKeys = rel.DefaultKeys(z2)
		sourceDistinct = false
	}

	return &sqlTable{r1.db, r1.tableName, colNames2, z2, cKeys, sourceDistinct, r1.err}

}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
func (r1 *sqlTable) Restrict(p rel.Predicate) rel.Relation {
	// TODO(jonlawlor): rewrite through to sql server
	return rel.NewRestrict(r1, p)
}

// Rename creates a new relation with new column names
// this can be handled during the scanner call
func (r1 *sqlTable) Rename(z2 interface{}) rel.Relation {

	// TODO(jonlawlor): check that the rename has the same size

	e2 := reflect.TypeOf(z2)

	// figure out the new names
	names2 := rel.FieldNames(e2)

	// create a map from the old names to the new names if there is any
	// difference between them
	nameMap := make(map[rel.Attribute]rel.Attribute)
	for i, att := range rel.Heading(r1) {
		nameMap[att] = names2[i]
	}

	cKeys1 := r1.cKeys
	cKeys2 := make(rel.CandKeys, len(cKeys1))
	// for each of the candidate keys, rename any keys from the old names to
	// the new ones
	for i := range cKeys1 {
		cKeys2[i] = make([]rel.Attribute, len(cKeys1[i]))
		for j, key := range cKeys1[i] {
			cKeys2[i][j] = nameMap[key]
		}
	}
	// order the keys
	rel.OrderCandidateKeys(cKeys2)

	return &sqlTable{r1.db, r1.tableName, r1.colNames, z2, cKeys2, r1.sourceDistinct, r1.err}

}

// Union creates a new relation by unioning the bodies of both inputs
func (r1 *sqlTable) Union(r2 rel.Relation) rel.Relation {
	// TODO(jonlawlor): if both r1 and r2 are on the same server, pass it
	// through to the source database.
	return rel.NewUnion(r1, r2)
}

// Diff creates a new relation by set minusing the two inputs
func (r1 *sqlTable) Diff(r2 rel.Relation) rel.Relation {
	// TODO(jonlawlor): if both r1 and r2 are on the same server, pass it
	// through to the source database.
	return rel.NewDiff(r1, r2)
}

// Join creates a new relation by performing a natural join on the inputs
func (r1 *sqlTable) Join(r2 rel.Relation, zero interface{}) rel.Relation {
	// TODO(jonlawlor): if both r1 and r2 are on the same server, pass it
	// through to the source database.
	return rel.NewJoin(r1, r2, zero)
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *sqlTable) GroupBy(t2, gfcn interface{}) rel.Relation {
	// TODO(jonlawlor): determine a way to pass through
	return rel.NewGroupBy(r1, t2, gfcn)
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *sqlTable) Map(mfcn interface{}, ckeystr [][]string) rel.Relation {
	// TODO(jonlawlor): determine a way to pass through
	return rel.NewMap(r1, mfcn, ckeystr)
}

// Error returns an error encountered during construction or computation
func (r1 *sqlTable) Err() error {
	return r1.err
}
