// Copyright (c) 2022 Alexandru Catrina
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
package libcsv

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestReadingIncorrectAddup(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Errorf("expected to fail but got %v", err)
		}
	}()

	New(strings.NewReader(`a,b,118 Casă și curățenie + 16.15 Alimente,2019-12-05,-27.73`))
}

func TestReadingIncorrectDate(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Errorf("expected to fail but got %v", err)
		}
	}()

	New(strings.NewReader(`a,b,118 Casă și curățenie + 16.15 Alimente,2019'12'05,-27.73`))
}

func TestReadingIncorrectAmount(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Errorf("expected to fail but got %v", err)
		}
	}()

	New(strings.NewReader(`a,b,118 Casă și curățenie + 16.15 Alimente,2019-12-05,-27x73`))
}

func TestNothingToDo(t *testing.T) {
	if all := New(strings.NewReader(`a,b,c,2019-12-05,100`)); len(all) == 1 {
		if out, _ := all.Filter(""); len(out) != 1 {
			t.Error("expected one record but got different")
		}
	}
}

var sample = `
Alexandru,(hypermarket),Apa,2019-10-03,-30.43
Alexandru,(magazin),?,2019-10-08,-349.00
Alexandru,(magazin),Produse Online,2019-10-14,-993.93
Ordonator,Alexandru,Transfer,2019-10-15,1000.00
Alexandru,Catrina,Împrumut,2019-10-16,-1500.00
Alexandru,Beneficiar #1,Chirie,2019-10-16,-1000.00
Alexandru,(dentist),Vizită dentist,2019-10-16,-750.00
Ordonator,Catrina,Transfer,2019-10-18,99999.99
Alexandru,(dentist),Vizită dentist,2019-10-23,-850.00
Alexandru,(magazin),?,2019-10-23,-104.23
Alexandru,(hypermarket),?,2019-10-23,-104.23
Alexandru,Beneficiar #2,?,2019-10-29,-55920.00
Alexandru,(online),Abonamente,2019-11-03,-30.46
Alexandru,(dentist),Vizită dentist,2019-11-04,-200.00
Alexandru,(stație de alimentare),Combustibil,2019-11-11,-200.30
Alexandru,(dentist),Vizită dentist,2019-11-18,-400.00
Ordonator,Alexandru,Transfer,2019-11-19,1000.00
Alexandru,(magazin),?,2019-11-21,-139.65
Alexandru,(hypermarket),?,2019-11-22,-28.20
Alexandru,(cafenea),Cafea,2019-11-22,-23.80
Ordonator,Alexandru,Transfer,2019-11-27,9000.00
Alexandru,(bucătar),Catering,2019-12-04,-40.00
Alexandru,(magazin),Sucuri,2019-12-04,-15.00
Alexandru,(magazin),11.58 Casă și curățenie + 16.15 Alimente,2019-12-05,-27.73
Catrina,(supermarket),Alimente,2019-12-06,-56.88
Alexandru,(hypermarket),139.94 Alimente + 58.35 Apă,2019-12-07,-198.29
Catrina,(supermarket),?,2019-12-07,-62.82
Catrina,(hypermarket),16.60 ? + 139.94 Alimente,2019-12-09,-156.54
Alexandru,(supermarket),Alimente,2019-12-09,-18.42
Alexandru,(hypermarket),Băcănie,2019-12-11,-186.20
Alexandru,(restaurant),Catering,2019-12-12,-45.50
Alexandru,(hypermarket),?,2019-12-12,-15.30
Alexandru,(taxi),Transport,2019-12-12,-10.00
Alexandru,(supermarket),38.76 Alimente + 301.70 Alimente,2020-01-10,-340.46
Alexandru,(hypermarket),12.00 Dulciuri + 162.37 Dulciuri,2020-01-10,-174.37
Catrina,(magazin),?,2020-01-11,-4022
Catrina,(magazin),?,2020-01-11,-9861
`

func TestReadingCSV(t *testing.T) {
	if all := New(strings.NewReader(sample)); len(all) != 42 {
		t.Errorf("doesn't match nr of records %v\n", len(all))
	}
}

var collection = New(strings.NewReader(sample))

func TestVariousStringFilters(t *testing.T) {
	collection.Filter(`[]`)
	collection.Filter(`  ( )`)
	collection.Filter(`[  ]+()`)
	collection.Filter(`  [   )- (             ] +[]+(] `)
}

func TestUnsupportedOperator(t *testing.T) {
	if _, err := collection.Filter(`( ) *[]`); err == nil {
		t.Error("expected filter to fail because of unsupported operator")
	}
}

func TestVariousSenders(t *testing.T) {
	if rs, _ := collection.Filter("[a=alex]"); len(rs) != 32 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	} else {
		for _, each := range rs {
			if !strings.HasPrefix(strings.ToLower(each.Sender), "alex") {
				t.Errorf("record doesn't have expected sender")
			}
		}
	}

	if rs, _ := collection.Filter("[a=alex,catrina]"); len(rs) != 38 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	} else {
		for _, each := range rs {
			if strings.HasPrefix(strings.ToLower(each.Sender), "alex") || strings.ToLower(each.Sender) == "catrina" {
				continue
			}

			t.Errorf("record doesn't have expected sender")
		}
	}

	if rs, _ := collection.Filter("[a=alexandrucatrina]"); len(rs) != 0 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}

	if rs, _ := collection.Filter(`[a = "Ordonator"]`); len(rs) != 4 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	} else {
		for _, each := range rs {
			if each.Sender != "Ordonator" {
				t.Errorf("record doesn't have expected sender")
			}
		}
	}
}

func TestVariousReceivers(t *testing.T) {
	if rs, _ := collection.Filter(`[b="Catrina"]`); len(rs) != 2 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	} else {
		for _, each := range rs {
			if each.Receiver != "Catrina" {
				t.Errorf("record doesn't have expected receiver")
			}
		}
	}

	if rs, _ := collection.Filter(`[b=alex]`); len(rs) != 3 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	} else {
		for _, each := range rs {
			if !strings.HasPrefix(strings.ToLower(each.Receiver), "alex") {
				t.Errorf("record doesn't have expected receiver")
			}
		}
	}

	if rs, _ := collection.Filter(`[b=beneficiar]`); len(rs) != 2 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	} else {
		for _, each := range rs {
			if !strings.HasPrefix(each.Receiver, "Beneficiar") {
				t.Errorf("record doesn't have expected receiver")
			}
		}
	}

	if rs, _ := collection.Filter(`[b=magazin]`); len(rs) != 9 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	} else {
		for _, each := range rs {
			if each.Receiver != "(magazin)" {
				t.Errorf("record doesn't have expected receiver")
			}
		}
	}
}

func TestWrongQueryFormulas(t *testing.T) {
	var err error

	_, err = collection.Filter(`[b=(magazin)]`)
	if err.Error() != "unsupported nested paranthesis in b=(magazin" {
		t.Errorf("expected fail but got %v", err)
	}

	_, err = collection.Filter(`[b=(magazin]`)
	if err.Error() != "number of opened paranthesis don't match with closed ones" {
		t.Errorf("expected fail but got %v", err)
	}

	_, err = collection.Filter(`[b=magazin)]`)
	if err.Error() != "number of opened paranthesis don't match with closed ones" {
		t.Errorf("expected fail but got %v", err)
	}

	_, err = collection.Filter(`[b=magazin) + [x=orice]]`)
	if err.Error() != "number of opened paranthesis don't match with closed ones" {
		t.Errorf("expected fail but got %v", err)
	}

	_, err = collection.Filter(`[b=magazin) + [x=[orice]]`)
	if err.Error() != "unsupported nested paranthesis in x=[orice" {
		t.Errorf("expected fail but got %v", err)
	}
}

func TestEitherSenderOrReceiver(t *testing.T) {
	if rs, _ := collection.Filter(`[x=catrina]`); len(rs) != 8 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	} else {
		for _, each := range rs {
			if each.Sender == "Catrina" || each.Receiver == "Catrina" {
				continue
			}

			t.Errorf("record doesn't have expected receiver or sender")
		}
	}
}

func TestVariousLabels(t *testing.T) {
	Setup(&Locale{Unicode: map[string]string{"î": "i"}})

	if rs, _ := collection.Filter(`[c=alimente]`); len(rs) != 7 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	} else {
		for _, each := range rs {
			if each.Label != "Alimente" {
				t.Errorf("record doesn't have expected label")
			}
		}
	}

	if rs, _ := collection.Filter(`[c=imprumut,cafea]`); len(rs) != 2 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	} else {
		for _, each := range rs {
			if each.Label == "Împrumut" || each.Label == "Cafea" {
				continue
			}

			t.Errorf("record doesn't have expected label")
		}
	}

	if rs, _ := collection.Filter(`[c=?]`); len(rs) != 11 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}
}

func TestDefaultSetupCalendaristicLookup(t *testing.T) {
	results, _ := collection.Filter("[d=noiembrie 2019]")
	if len(results) != 0 {
		t.Errorf("unexpected nr of results %d\n", len(results))
	}
}

var calendar = []string{
	"ianuarie",
	"februarie",
	"martie",
	"aprilie",
	"mai",
	"iunie",
	"iulie",
	"august",
	"septembrie",
	"octombrie",
	"noiembrie",
	"decembrie",
}

func TestCalendaristicLookup(t *testing.T) {
	Setup(&Locale{Months: calendar}) // set locale months

	results, _ := collection.Filter("[d=noiembrie 2019]")
	if len(results) != 9 {
		t.Errorf("unexpected nr of results %d\n", len(results))
	} else {
		for _, each := range results {
			if each.Date.Year() != 2019 {
				t.Error("got a different year")
			}
			if each.Date.Month() != time.November {
				t.Error("got a different month")
			}
		}
	}
}

func TestVariousDateLookups(t *testing.T) {
	// yyyy mm dd
	if rs, _ := collection.Filter("[d=2020-01-10]"); len(rs) != 4 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}
	if rs, _ := collection.Filter("[d=2020-01-11]"); len(rs) != 2 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}

	// dd mm yyyy
	if rs, _ := collection.Filter("[d=10-01-2020]"); len(rs) != 4 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}
	if rs, _ := collection.Filter("[d=11-01-2020]"); len(rs) != 2 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}
	if rs, _ := collection.Filter("[d=10.01.2020]"); len(rs) != 4 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}
	if rs, _ := collection.Filter("[d=11.01.2020]"); len(rs) != 2 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}
	if rs, _ := collection.Filter("[d=10/01/2020]"); len(rs) != 4 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}
	if rs, _ := collection.Filter("[d=11/01/2020]"); len(rs) != 2 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}

	// month yyyy
	if rs, _ := collection.Filter("[d=ianuarie 2020]"); len(rs) != 6 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}

	// dd month yyyy
	if rs, _ := collection.Filter("[d = 29 octombrie 2019]"); len(rs) != 1 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}

	// dd month
	present := time.Now()
	now := time.Date(present.Year(), present.Month(), present.Day(), 0, 0, 0, 0, time.UTC)
	collection2 := append(collection, Record{"a", "b", "c", now, 100})
	currentMonth := int(now.Month())
	currentMonthLocale := calendar[currentMonth-1]
	formula := fmt.Sprintf("[d = %v %v]", now.Day(), currentMonthLocale)
	if rs, _ := collection2.Filter(formula); len(rs) != 1 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}

	// yyyy
	if rs, _ := collection.Filter("[d=2019]"); len(rs) != 36 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}
}

func TestDateIntervals(t *testing.T) {
	Setup(&Locale{Months: calendar}) // set months

	if rs, _ := collection.Filter("(d > noiembrie 2019)"); len(rs) != 21 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}
	if rs, _ := collection.Filter("[d > noiembrie 2019]"); len(rs) != 30 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}
	if rs, _ := collection.Filter("(d > noiembrie 2019; d < decembrie 2019]"); len(rs) != 15 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}
	if rs, _ := collection.Filter("[d > noiembrie 2019; d < 2020)"); len(rs) != 24 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}
}

func TestVariousAmountConditions(t *testing.T) {
	if rs, _ := collection.Filter("[s>0]"); len(rs) != 42 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}
	if rs, _ := collection.Filter("[s>0; z>0]"); len(rs) != 4 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}
	if rs, _ := collection.Filter("[s>0; z<0]"); len(rs) != 38 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}
	if rs, _ := collection.Filter("[s<1000; z<0]"); len(rs) != 36 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}
	if rs, _ := collection.Filter("[s<1000; z>0]"); len(rs) != 2 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}
	if rs, _ := collection.Filter("(s<1000; z>0)"); len(rs) != 0 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	}
	if rs, _ := collection.Filter("[s>1000; z<0]"); len(rs) != 3 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	} else {
		for _, each := range rs {
			if each.Amount > -1_000_00 {
				t.Errorf("expected amounts to be under 1 000.00 but got %v", each.Amount)
			}
		}
	}
	if rs, _ := collection.Filter("(s>1000; z<0)"); len(rs) != 2 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	} else {
		for _, each := range rs {
			if each.Amount >= -1_000_00 {
				t.Errorf("expected amounts to be under 1 000.00 but got %v", each.Amount)
			}
		}
	}

	if rs, _ := collection.Filter("(s>1000)"); len(rs) != 4 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	} else {
		for _, each := range rs {
			var amount int64
			if each.Amount < 0 {
				amount = -each.Amount
			} else {
				amount = each.Amount
			}

			if amount <= 1_000_00 {
				t.Errorf("expected amounts to be above 1 000.00 but got %v", each.Amount)
			}
		}
	}

	if rs, _ := collection.Filter("[s=1000]"); len(rs) != 3 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	} else {
		for _, each := range rs {
			if each.Amount == 1_000_00 || each.Amount == -1_000_00 {
				continue
			}

			t.Errorf("expected absolute value of amount to be 1 000.00 but got %v", each.Amount)
		}
	}

	if rs, _ := collection.Filter("[s=1000; z>0]"); len(rs) != 2 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	} else {
		for _, each := range rs {
			if each.Amount == 1_000_00 {
				continue
			}

			t.Errorf("expected value of amount to be (positive) 1 000.00 but got %v", each.Amount)
		}
	}
	if rs, _ := collection.Filter("(s=40,22)"); len(rs) != 1 {
		t.Errorf("unexpected nr of results %d\n", len(rs))
	} else {
		if rs[0].Amount != -4022 {
			t.Error("unexpected amount")
		}
	}
}

func TestVariousIncorrectFormulas(t *testing.T) {
	var err error

	_, err = collection.Filter("+[]")
	if err.Error() != "incorrect query +[]" {
		t.Error("expected fail but didn't")
	}

	_, err = collection.Filter("[] []")
	if !strings.HasPrefix(err.Error(), "unexpected operation between collections") {
		t.Error("expected fail but didn't")
	}

	_, err = collection.Filter("[] + []+")
	if !strings.HasPrefix(err.Error(), "expected opening parenthesis after operator in +") {
		t.Error("expected fail but didn't")
	}

	_, err = collection.Filter("[a>alex]")
	if err.Error() != "header a? 62" {
		t.Error("expected fail but didn't")
	}

	_, err = collection.Filter("[b>alex]")
	if err.Error() != "header b? 62" {
		t.Error("expected fail but didn't")
	}

	_, err = collection.Filter("[c>alex]")
	if err.Error() != "header c? 62" {
		t.Error("expected fail but didn't")
	}

	_, err = collection.Filter("[x>alex]")
	if err.Error() != "header x? 62" {
		t.Error("expected fail but didn't")
	}

	_, err = collection.Filter("[z=0]")
	if err.Error() != "header z? 61" {
		t.Error("expected fail but didn't")
	}

	_, err = collection.Filter("[d:x]")
	if err.Error() != "unsupported header: 0" {
		t.Error("expected fail but didn't")
	}
}

func TestVariousFilters(t *testing.T) {
	if out, err := collection.Filter("[] - [a=alex]"); err != nil {
		t.Error(err)
	} else {
		for _, each := range out {
			if strings.HasPrefix(strings.ToLower(each.Sender), "alex") {
				t.Error("unexpected sender in union")
			}
		}
	}

	if out, err := collection.Filter("[a=catrina] + [b=catrina]"); err != nil {
		t.Error(err)
	} else {
		for _, each := range out {
			if each.Sender == "Catrina" || each.Receiver == "Catrina" {
				continue
			}

			t.Error("unexpected sender or receiver in union")
		}
	}

	if out, err := collection.Filter("[b=magazin; d=octombrie 2019] + [b=magazin; d=ianuarie 2020]"); err != nil {
		t.Error(err)
	} else if len(out) != 5 {
		t.Errorf("unexpected nr of records, got %v", len(out))
	} else {
		for _, each := range out {
			if each.Date.Month() == time.October && each.Date.Year() == 2019 {
				continue
			}
			if each.Date.Month() == time.January && each.Date.Year() == 2020 {
				continue
			}

			t.Error("unexpected record in union")
		}
	}
}
