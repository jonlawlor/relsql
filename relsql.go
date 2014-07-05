// Package relsql implements a rel.Relation object that uses sql.DB.
// The name is nonstandard go because callers would always have to use a type
// alias to construct one otherwise.
package relsql

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/jonlawlor/rel"
	"reflect"
	"strings"
	"text/template"
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

// sqlTable is an implementation of Relation using an sql.DB
type sqlTable struct {
	// the *sql.DB connection, produced by an sql driver
	db *sql.DB

	// tablename is the name of the table in the database
	tableName string

	// the columns available in the table
	colNames []string

	// colNames and zero should always represent the same number of fields

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

// selectStatement is a very simple sql select statement.  This will be
// replaced with a more general verion(s) to allow relsql to perform query
// rewrite using restrict, join, union, and diff.  I'm not sure if it will ever
// use rewrite for map and groupby.  Maybe It should depend upon sqlx, for the
// parameters?
type selectStatement struct {
	SourceDistinct bool
	ColNames       string
	TableName      string
}

// queryString constructs a query string from a selectStatement.
func (s *selectStatement) queryString() (str string, err error) {
	const selectTemplate = "SELECT{{if .SourceDistinct}} {{else}} DISTINCT {{end}}{{.ColNames}} FROM {{.TableName}}"
	var b bytes.Buffer
	t := template.Must(template.New("select").Parse(selectTemplate))
	err = t.Execute(&b, s)
	str = b.String()
	return
}

// TupleChan returns the tuples from the sql query represented by the relation
// in a channel.
func (r1 *sqlTable) TupleChan(t interface{}) chan<- struct{} {
	cancel := make(chan struct{})
	// reflect on the channel
	chv := reflect.ValueOf(t)
	err := rel.EnsureChan(chv.Type(), r1.zero)
	if err != nil {
		r1.err = err
		return cancel
	}
	if r1.err != nil {
		chv.Close()
		return cancel
	}
	go func(db *sql.DB, res reflect.Value) {
		// construct the select query string
		q, err := (&selectStatement{r1.sourceDistinct, strings.Join(r1.colNames, ", "), r1.tableName}).queryString()
		if err != nil {
			r1.err = err
			res.Close()
			return
		}

		// start a transaction
		tx, err := db.Begin()
		if err != nil {
			r1.err = err
			res.Close()
			return
		}

		// execute the query
		rows, err := tx.Query(q)

		if err != nil {
			r1.err = err
			res.Close()
			return
		}

		e1 := reflect.TypeOf(r1.zero)
		resSel := reflect.SelectCase{Dir: reflect.SelectSend, Chan: res}
		canSel := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(cancel)}
		n := e1.NumField()
		// assign the records to the result tuples
		for rows.Next() {

			// construct the result value
			tup := reflect.Indirect(reflect.New(e1))
			values := []interface{}{}

			for i := 0; i < n; i++ {
				values = append(values, tup.Field(i).Addr().Interface())
			}

			if err := rows.Scan(values...); err != nil {
				r1.err = err
				tx.Commit()
				res.Close()

				return
			}
			// send the value on the results channel, or cancel
			resSel.Send = tup
			chosen, _, _ := reflect.Select([]reflect.SelectCase{canSel, resSel})
			if chosen == 0 {
				// cancel has been closed, so close the query results
				tx.Commit()
				rows.Close()
				return
			}
		}
		tx.Commit()
		rows.Close()
		res.Close()
	}(r1.db, chv)

	return cancel
}

// Zero returns the zero value of the relation (a blank tuple)
func (r1 *sqlTable) Zero() interface{} {
	return r1.zero
}

// CKeys is the set of candidate keys in the relation
func (r1 *sqlTable) CKeys() rel.CandKeys {
	return r1.cKeys
}

// GoString returns a text representation of the Relation
func (r1 *sqlTable) GoString() string {
	return fmt.Sprintf("relsql.sqlTable{sql.DB, %s, %v, %v, %v, %v, %v}", r1.tableName, r1.colNames, r1.zero, r1.cKeys, r1.sourceDistinct, r1.err)
}

// String returns a text representation of the Relation
func (r1 *sqlTable) String() string {
	return "Relation(" + rel.HeadingString(r1) + ")"
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
