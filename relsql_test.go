//relsql_test implements some tests for sql based relations

package relcsv

import (
	"database/sql"
	"github.com/jonlawlor/rel"
	_ "github.com/mattn/go-sqlite3"
	"testing"
)

// test select query generation
func TestSelect(t *testing.T) {
	// generate a distinct and non distinct query

	var queryTest = []struct {
		statement *selectStatement
		query     string
	}{
		{&selectStatement{true, "foo, bar", "baz"}, "SELECT foo, bar FROM baz"},
		{&selectStatement{false, "foo", "baz"}, "SELECT DISTINCT foo FROM baz"},
	}
	for i, tt := range queryTest {
		if str, _ := tt.statement.queryString(); str != tt.query {
			t.Errorf("%d has queryString() => %v, want %v", i, str, tt.query)
		}
	}
}

// test database connection and tuple generation
func TestSQL(t *testing.T) {

	// this is adapted from the simple example from mattn's go-sqlite3 package
	// note: cache=shared is essential; relsql requires concurrent connections.
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	defer db.Close()

	// create an example table
	sql := `
	create table suppliers (SNO integer not null primary key, SName text, Status integer, City text);
	delete from suppliers;
	`

	_, err = db.Exec(sql)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	type supplierTup struct {
		SNO    int
		SName  string
		Status int
		City   string
	}
	suppliersRaw := []supplierTup{
		{1, "Smith", 20, "London"},
		{2, "Jones", 10, "Paris"},
		{3, "Blake", 30, "Paris"},
		{4, "Clark", 20, "London"},
		{5, "Adams", 30, "Athens"},
	}

	tx, err := db.Begin()
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	stmt, err := tx.Prepare("insert into suppliers(SNO, SName, Status, City) values(?, ?, ?, ?)")
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	defer stmt.Close()
	for _, rec := range suppliersRaw {
		_, err = stmt.Exec(rec.SNO, rec.SName, rec.Status, rec.City)
		if err != nil {
			t.Errorf(err.Error())
			return
		}
	}
	tx.Commit()

	// create a new relation from that table
	suppliers := New(db, "suppliers", supplierTup{}, [][]string{[]string{"SNO"}})

	// orders relation, with candidate keys {PNO, SNO}
	type orderTup struct {
		PNO int
		SNO int
		Qty int
	}

	orders := rel.New([]orderTup{
		{1, 1, 300},
		{1, 2, 200},
		{1, 3, 400},
		{1, 4, 200},
		{1, 5, 100},
		{1, 6, 100},
		{2, 1, 300},
		{2, 2, 400},
		{3, 2, 200},
		{4, 2, 200},
		{4, 4, 300},
		{4, 5, 400},
	}, [][]string{
		[]string{"PNO", "SNO"},
	})

	type distinctTup struct {
		SNO   int
		SName string
	}

	type nonDistinctTup struct {
		SName string
		City  string
	}

	type titleCaseTup struct {
		Sno    int
		SName  string
		Status int
		City   string
	}

	type joinTup struct {
		PNO    int
		SNO    int
		Qty    int
		SName  string
		Status int
		City   string
	}

	type groupByTup struct {
		City   string
		Status int
	}

	type valTup struct {
		Status int
	}

	groupFcn := func(val <-chan valTup) valTup {
		res := valTup{}
		for vi := range val {
			res.Status += vi.Status
		}
		return res
	}

	type mapRes struct {
		SNO     int
		SName   string
		Status2 int
		City    string
	}

	mapFcn := func(tup1 supplierTup) mapRes {
		return mapRes{tup1.SNO, tup1.SName, tup1.Status * 2, tup1.City}
	}

	mapKeys := [][]string{
		[]string{"SNO"},
	}

	var relTest = []struct {
		rel          rel.Relation
		expectString string
		expectDeg    int
		expectCard   int
	}{
		{suppliers, "Relation(SNO, SName, Status, City)", 4, 5},
		{suppliers.Restrict(rel.Attribute("SNO").EQ(1)), "σ{SNO == 1}(Relation(SNO, SName, Status, City))", 4, 1},
		{suppliers.Project(distinctTup{}), "Relation(SNO, SName)", 2, 5},
		{suppliers.Project(nonDistinctTup{}), "Relation(SName, City)", 2, 5},
		{suppliers.Rename(titleCaseTup{}), "Relation(Sno, SName, Status, City)", 4, 5},
		{suppliers.Diff(suppliers.Restrict(rel.Attribute("SNO").EQ(1))), "Relation(SNO, SName, Status, City) − σ{SNO == 1}(Relation(SNO, SName, Status, City))", 4, 4},
		{suppliers.Union(suppliers.Restrict(rel.Attribute("SNO").EQ(1))), "Relation(SNO, SName, Status, City) ∪ σ{SNO == 1}(Relation(SNO, SName, Status, City))", 4, 5},
		{suppliers.Join(orders, joinTup{}), "Relation(SNO, SName, Status, City) ⋈ Relation(PNO, SNO, Qty)", 6, 11},
		{suppliers.GroupBy(groupByTup{}, groupFcn), "Relation(SNO, SName, Status, City).GroupBy({City, Status}->{Status})", 2, 3},
		{suppliers.Map(mapFcn, mapKeys), "Relation(SNO, SName, Status, City).Map({SNO, SName, Status, City}->{SNO, SName, Status2, City})", 4, 5},
		{suppliers.Map(mapFcn, [][]string{}), "Relation(SNO, SName, Status, City).Map({SNO, SName, Status, City}->{SNO, SName, Status2, City})", 4, 5},
	}
	for i, tt := range relTest {
		if str := tt.rel.String(); str != tt.expectString {
			t.Errorf("%d has String() => %v, want %v", i, str, tt.expectString)
		}
		if deg := rel.Deg(tt.rel); deg != tt.expectDeg {
			t.Errorf("%d %s has Deg() => %v, want %v", i, tt.expectString, deg, tt.expectDeg)
		}
		if card := rel.Card(tt.rel); card != tt.expectCard {
			t.Errorf("%d %s has Card() => %v, want %v", i, tt.expectString, card, tt.expectCard)
		}
		if err := tt.rel.Err(); err != nil {
			t.Errorf("%d has Err() => %v", i, err.Error())
		}

	}
}
