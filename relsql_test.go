//relsql_test implements some tests for sql based relations

package relcsv

import "testing"

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
