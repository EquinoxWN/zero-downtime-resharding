package schema

import "testing"

func TestOnlyPlainTableNamesAreAccepted(t *testing.T) {
	for _, tb := range Tables {
		if q, err := tb.QuotedName(); err != nil || q != `"`+tb.Name+`"` {
			t.Fatalf("%s: %q %v", tb.Name, q, err)
		}
	}
	for _, bad := range []string{"", "Orders", "orders; drop table accounts", `or"ders`, "1orders", "public.orders"} {
		if _, err := (Table{Name: bad}).QuotedName(); err == nil {
			t.Errorf("%q should be refused", bad)
		}
	}
}
