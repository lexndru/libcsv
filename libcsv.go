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
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	OPT_MAX_READ    int64  = 1 << 20
	OPT_DATE_LAYOUT string = "2006-01-02"
	OPT_SEPARATOR   string = "+"
)

type Locale struct {
	Months  []string
	Unicode map[string]string
}

var locale = &Locale{
	Months:  make([]string, 0),
	Unicode: make(map[string]string),
}

func (lc *Locale) Month(monthName string) int {
	for i, m := range lc.Months {
		if strings.HasPrefix(m, monthName) {
			return i
		}
	}

	return -1
}

func (lc *Locale) Translate(text string) string {
	for chr, val := range lc.Unicode {
		text = strings.ReplaceAll(text, chr, val)
	}

	return text
}

func Setup(lc *Locale) {
	locale = lc
}

var whitespace = regexp.MustCompile(`\s+`)

func clean(s string) string {
	return whitespace.ReplaceAllString(strings.TrimSpace(s), " ")
}

func mustParseDate(row []string, index int) time.Time {
	val, err := time.Parse(OPT_DATE_LAYOUT, clean(row[index]))

	if err != nil {
		throw(err, row)
	}

	return val
}

func mustParseAmount(row []string, index int) int64 {
	str := strings.ReplaceAll(clean(row[index]), ".", "")
	val, err := strconv.ParseInt(str, 10, 64)

	if err != nil {
		throw(err, row)
	}

	return val
}

func throw(e error, r []string) {
	panic(fmt.Errorf("%v => %v", e, r))
}

type Record struct {
	Sender   string
	Receiver string
	Label    string
	Date     time.Time
	Amount   int64 // sum
}

func (r Record) String() string {
	return fmt.Sprintf(`["%v","%v","%v",%v,%v]`, r.Sender, r.Receiver, r.Label, r.Date.Unix(), r.Amount)
}

type Collection []Record

func New(src io.Reader) Collection {
	collection := make(Collection, 0)
	reader := csv.NewReader(io.LimitReader(src, OPT_MAX_READ))

	for {
		if row, err := reader.Read(); err == io.EOF {
			break
		} else if strings.Contains(row[2], OPT_SEPARATOR) {
			sum := mustParseAmount(row, 4)
			var k int64 = 1
			if sum < 0 {
				k = -1
			}

			var acc int64
			for _, each := range strings.Split(row[2], OPT_SEPARATOR) {
				pairs := strings.SplitN(clean(each), " ", 2)
				subtotal := mustParseAmount(pairs, 0) * k
				collection = append(collection, Record{
					Sender:   clean(row[0]),
					Receiver: clean(row[1]),
					Label:    clean(pairs[1]), // new label
					Date:     mustParseDate(row, 3),
					Amount:   subtotal,
				})

				acc += subtotal
			}

			if diff := sum - acc; diff != 0 {
				throw(fmt.Errorf("doesn't add up %v", diff), row)
			}
		} else {
			collection = append(collection, Record{
				Sender:   clean(row[0]),
				Receiver: clean(row[1]),
				Label:    clean(row[2]),
				Date:     mustParseDate(row, 3),
				Amount:   mustParseAmount(row, 4),
			})
		}
	}

	return collection
}

const (
	_UNION = '+'
	_DIFF  = '-'
)

func (c Collection) Filter(q string) (results Collection, err error) {
	var stack = make([]token, 0)
	err = compile(clean(q), &stack)

	if err != nil {
		return nil, err
	} else if len(stack) == 0 {
		return c, nil // nothing to do?
	}

	_mem := make(map[string]Record)
	if start := stack[0]; start.IsFormula() {
		cScope := scope{start.flags&0b10 != 0, start.flags&0b01 != 0}
		if filters, err := prepare(&cScope, start.value); err != nil {
			return nil, err
		} else if out, err := query(c, filters); err != nil {
			return nil, err
		} else {
			for _, r := range out {
				k := r.String()
				_mem[k] = r
				results = append(results, r)
			}
		}
	} else {
		return results, fmt.Errorf("incorrect query %v", q)
	}

	for i := 1; i < len(stack); i += 2 {
		op, ls := stack[i], stack[i+1]

		if op.IsFormula() {
			return results, fmt.Errorf("incorrect query, missing operation %v", op.value)
		} else if !ls.IsFormula() {
			return results, fmt.Errorf("incorrect query, missing formula %v", op.value)
		}

		filters, err := prepare(&scope{ls.flags&0b10 != 0, ls.flags&0b01 != 0}, ls.value)
		if err != nil {
			return nil, err
		}

		switch op.value[0] {
		case _UNION:
			out, err := query(c, filters)
			if err != nil {
				return nil, err
			}

			for _, r2 := range out {
				var r2k = r2.String()
				if _, ok := _mem[r2k]; !ok {
					results = append(results, r2)
					_mem[r2k] = r2
				}
			}
		case _DIFF:
			out, err := query(results, filters)
			if err != nil {
				return nil, err
			}

			for _, r2 := range out {
				delete(_mem, r2.String())
			}

			out2 := make([]Record, 0, len(results)-len(out))
			for _, val := range _mem {
				out2 = append(out2, val)
			}

			results = out2 // ?
		default:
			return results, fmt.Errorf("unsupported operator: %v", op.value[0])
		}
	}

	sort.SliceStable(results, func(i, j int) bool {
		if results[i].Date.Equal(results[j].Date) {
			return results[i].Amount < results[j].Amount
		}

		return results[i].Date.After(results[j].Date)
	})

	return results, nil
}

/******************************* internals ***********************************/

const (
	_OP_SQ = '['
	_OP_RD = '('
	_CL_SQ = ']'
	_CL_RD = ')'
)

type token struct {
	value []byte
	flags int
	class int
}

func (t token) IsFormula() bool {
	return t.class == 1
}

func compile(str string, stack *[]token) error {
	if len(str) == 0 {
		return nil
	}

	if len(*stack) == 0 {
		opCount := strings.Count(str, string(_OP_RD)) + strings.Count(str, string(_OP_SQ))
		clCount := strings.Count(str, string(_CL_RD)) + strings.Count(str, string(_CL_SQ))
		if opCount != clCount {
			return fmt.Errorf("number of opened paranthesis don't match with closed ones")
		}
	}

	if chr := str[0]; chr == _OP_SQ || chr == _OP_RD {
		clsq := strings.IndexRune(str, _CL_SQ)
		clrd := strings.IndexRune(str, _CL_RD)

		var cl int
		if clsq > -1 && clrd > -1 {
			cl = min(clsq, clrd)
		} else if clsq == -1 {
			cl = clrd
		} else {
			cl = clsq
		}

		if cl == -1 {
			return fmt.Errorf("formula %v does't have a closing parenthesis", str)
		}

		var flags int

		if chr == _OP_SQ {
			flags |= 2
		}

		if str[cl] == _CL_SQ {
			flags |= 1
		}

		value := clean(str[1:cl])
		ftoken := token{
			value: []byte(value),
			flags: flags,
			class: 1,
		}

		*stack = append(*stack, ftoken)
		if len(str[cl+1:]) > 0 {
			return compile(str[cl+1:], stack)
		}
	} else {
		opsq := strings.IndexRune(str, _OP_SQ)
		oprd := strings.IndexRune(str, _OP_RD)

		var op int
		if opsq > -1 && oprd > -1 {
			op = min(opsq, oprd)
		} else if opsq == -1 {
			op = oprd
		} else {
			op = opsq
		}

		if op == -1 {
			if strings.IndexRune(str, _CL_SQ)+strings.IndexRune(str, _CL_RD) > -2 && len(*stack) > 0 {
				return fmt.Errorf("unsupported nested paranthesis in %s", (*stack)[len(*stack)-1].value)
			}

			return fmt.Errorf("expected opening parenthesis after operator in %v", str)
		}

		operator := clean(str[:op])
		if len(operator) != 1 {
			return fmt.Errorf("unexpected operation between collections: %v", operator)
		}

		otoken := token{[]byte(operator), 0, 0}
		*stack = append(*stack, otoken)

		if len(str[op:]) > 0 {
			return compile(str[op:], stack)
		}
	}

	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}

	return b
}

const (
	HEADER_A_SENDER   byte = 'a'
	HEADER_B_RECEIVER byte = 'b'
	HEADER_C_CATEGORY byte = 'c'
	HEADER_D_DATE     byte = 'd'
	HEADER_S_SUM      byte = 's'
	HEADER_X_ANYONE   byte = 'x' // hidden header, "either sender or receiver" is ORing trx party
	HEADER_0_BALANCE  byte = 'z' // hidden header, "by reference to zero" is positive or negative
)

const (
	OPERATOR_EQUAL_MATCH  byte = '='
	OPERATOR_GREATER_THAN byte = '>'
	OPERATOR_LESS_THAN    byte = '<'
)

type scope struct {
	isLeftInclusive  bool
	isRightInclusive bool
}

var nonAlphaNumeric = regexp.MustCompile(`[^a-z0-9]`)

func doesItMatch(keyword string, value string) bool {
	lastIndex := len(keyword) - 1
	asciiKeyword := locale.Translate(strings.ToLower(keyword))
	asciiLookupValue := locale.Translate(strings.ToLower(value))

	if asciiKeyword[0] == '"' && asciiKeyword[lastIndex] == '"' {
		return asciiLookupValue == asciiKeyword[1:lastIndex]
	}

	if strings.HasPrefix(asciiLookupValue, asciiKeyword) {
		return true
	}

	az09 := strings.TrimSpace(nonAlphaNumeric.ReplaceAllString(asciiLookupValue, " "))

	return strings.HasPrefix(az09, asciiKeyword)
}

var _TEXT_OR_SEP = []byte(",")

type comparator struct {
	header   byte
	operator byte

	bytesValue  []byte // sender, receiver, label
	numberValue int64  // timestamp, amount
	offsetValue int64  // range for timestamp, amount to calc. aprox. values

	intervalScope *scope
}

func (c comparator) isMatchingText(value string) bool {
	for _, v := range bytes.Split(c.bytesValue, _TEXT_OR_SEP) {
		if doesItMatch(string(v), value) {
			return true
		}
	}

	return false
}

func (c comparator) IsMatchingSender(r Record) bool {
	return c.isMatchingText(r.Sender)
}

func (c comparator) IsMatchingReceiver(r Record) bool {
	return c.isMatchingText(r.Receiver)
}

func (c comparator) IsMatchingSenderOrReceiver(r Record) bool {
	return c.isMatchingText(r.Sender) || c.isMatchingText(r.Receiver)
}

func (c comparator) IsMatchingLabel(r Record) bool {
	return c.isMatchingText(r.Label)
}

func (c comparator) IsMatchingDate(r Record) bool {
	if c.offsetValue > 0 {
		return r.Date.Unix() >= c.numberValue && r.Date.Unix() <= c.numberValue+c.offsetValue
	}

	return r.Date.Unix() == c.numberValue
}

func (c comparator) IsAfterDate(r Record) bool {
	if c.intervalScope.isLeftInclusive {
		return r.Date.Unix() >= c.numberValue
	}

	return r.Date.Unix() > c.numberValue+c.offsetValue
}

func (c comparator) IsBeforeDate(r Record) bool {
	if c.intervalScope.isRightInclusive {
		return r.Date.Unix() <= c.numberValue+c.offsetValue
	}

	return r.Date.Unix() < c.numberValue
}

func (c comparator) IsMatchingAmount(r Record) bool {
	var amount int64
	if r.Amount < 0 {
		amount = -r.Amount
	} else {
		amount = r.Amount
	}

	if c.offsetValue > 0 {
		return amount >= c.numberValue && amount <= c.numberValue+c.offsetValue
	}

	return amount == c.numberValue
}

func (c comparator) IsGreaterThanAmount(r Record) bool {
	var amount int64
	if r.Amount < 0 {
		amount = -r.Amount
	} else {
		amount = r.Amount
	}

	if c.intervalScope.isLeftInclusive {
		return amount >= c.numberValue
	}

	return amount > c.numberValue
}

func (c comparator) IsLessThanAmount(r Record) bool {
	var amount int64
	if r.Amount < 0 {
		amount = -r.Amount
	} else {
		amount = r.Amount
	}

	if c.intervalScope.isRightInclusive {
		return amount <= c.numberValue
	}

	return amount < c.numberValue
}

func (c comparator) HasAscendingAmount(r Record) bool {
	return r.Amount > c.numberValue
}

func (c comparator) HasDescendingAmount(r Record) bool {
	return r.Amount < c.numberValue
}

func (c comparator) Compare(r Record) (bool, error) {
	switch c.header {
	case HEADER_A_SENDER:
		switch c.operator {
		case OPERATOR_EQUAL_MATCH:
			return c.IsMatchingSender(r), nil
		default:
			return false, fmt.Errorf("header a? %v", c.operator)
		}
	case HEADER_B_RECEIVER:
		switch c.operator {
		case OPERATOR_EQUAL_MATCH:
			return c.IsMatchingReceiver(r), nil
		default:
			return false, fmt.Errorf("header b? %v", c.operator)
		}
	case HEADER_C_CATEGORY:
		switch c.operator {
		case OPERATOR_EQUAL_MATCH:
			return c.IsMatchingLabel(r), nil
		default:
			return false, fmt.Errorf("header c? %v", c.operator)
		}
	case HEADER_D_DATE:
		switch c.operator {
		case OPERATOR_EQUAL_MATCH:
			return c.IsMatchingDate(r), nil
		case OPERATOR_GREATER_THAN:
			return c.IsAfterDate(r), nil
		case OPERATOR_LESS_THAN:
			return c.IsBeforeDate(r), nil
		default:
			return false, fmt.Errorf("header d? %v", c.operator)
		}
	case HEADER_S_SUM:
		switch c.operator {
		case OPERATOR_EQUAL_MATCH:
			return c.IsMatchingAmount(r), nil
		case OPERATOR_GREATER_THAN:
			return c.IsGreaterThanAmount(r), nil
		case OPERATOR_LESS_THAN:
			return c.IsLessThanAmount(r), nil
		default:
			return false, fmt.Errorf("header s? %v", c.operator)
		}
	case HEADER_X_ANYONE:
		switch c.operator {
		case OPERATOR_EQUAL_MATCH:
			return c.IsMatchingSenderOrReceiver(r), nil
		default:
			return false, fmt.Errorf("header x? %v", c.operator)
		}
	case HEADER_0_BALANCE:
		switch c.operator {
		case OPERATOR_GREATER_THAN:
			return c.HasAscendingAmount(r), nil
		case OPERATOR_LESS_THAN:
			return c.HasDescendingAmount(r), nil
		default:
			return false, fmt.Errorf("header z? %v", c.operator)
		}
	}

	return false, fmt.Errorf("unsupported header: %v", c.header)
}

var (
	_FORMULA_REGEX = regexp.MustCompile(`\s*([xzabcds]\s*[=><])\s*(.+)\s*`)
	_FORMUAL_PARTS = 2
)

var _DELIM = []byte(";") // (a = alex; s > 5000; ...)

var (
	_DATE_REGEX_YYYY_MM_DD    = regexp.MustCompile(`^(\d{4})-(\d{2})-(\d{2})$`)
	_DATE_REGEX_DD_MM_YYYY    = regexp.MustCompile(`^(\d{1,2})[\/\.\-](\d{1,2})[\/\.\-](\d{4})$`)
	_DATE_REGEX_MONTH_YYYY    = regexp.MustCompile(`^(\w{3,})\s+(\d{4})$`)
	_DATE_REGEX_DD_MONTH_YYYY = regexp.MustCompile(`^(\d{1,2})\s+(\w{3,})\s+(\d{4})$`)
	_DATE_REGEX_DD_MONTH      = regexp.MustCompile(`^(\d{1,2})\s+(\w{3,})$`) // consider current year or last year
)

const _MIN_YEAR = 1922 // 100 years ago

func prepare(cs *scope, cleanQuery []byte) ([]comparator, error) {
	conditions := bytes.Split(bytes.TrimSpace(cleanQuery), _DELIM)
	filters := make([]comparator, 0, len(conditions))

	for _, condition := range conditions {
		if len(condition) == 0 {
			break // avoid useless conditions
		}

		var tokens = _FORMULA_REGEX.FindSubmatch(condition)
		var comp = comparator{intervalScope: cs}

		if len(tokens) == _FORMUAL_PARTS+1 { // +1 because FindSubmatch includes the string itself
			field, value := bytes.ReplaceAll(tokens[1], []byte(" "), []byte("")), bytes.ToLower(tokens[2])

			comp.header = field[0]
			comp.operator = field[1]
			comp.bytesValue = bytes.TrimSpace(value)

			switch comp.header {
			case HEADER_D_DATE: // order of most likely to be used
				if dt := _DATE_REGEX_DD_MONTH.FindSubmatch(comp.bytesValue); len(dt) == 3 {
					dayOfMonth, monthName := string(dt[1]), string(dt[2])

					if day, err := strconv.ParseInt(dayOfMonth, 10, 8); err != nil {
						return nil, fmt.Errorf("not a day %v: %v", dayOfMonth, err)
					} else if day > 0 && day < 32 {
						currentMonthIndex := time.Now().Month()
						monthIndex := locale.Month(monthName) + 1

						if monthIndex > 0 {
							currentYear := time.Now().Year()
							if monthIndex > int(currentMonthIndex) {
								currentYear -= 1 // if month is in the future, use last year
							}

							datetime := time.Date(currentYear, time.Month(monthIndex), int(day), 0, 0, 0, 0, time.UTC)
							comp.numberValue = datetime.Unix()
						}
					}
				} else if dt := _DATE_REGEX_DD_MONTH_YYYY.FindSubmatch(comp.bytesValue); len(dt) == 4 {
					dayOfMonth, monthName, fullYear := string(dt[1]), string(dt[2]), string(dt[3])

					if year, err := strconv.ParseInt(fullYear, 10, 16); err != nil {
						return nil, fmt.Errorf("not a year %v: %v", fullYear, err)
					} else if day, err := strconv.ParseInt(dayOfMonth, 10, 8); err != nil {
						return nil, fmt.Errorf("not a month %v: %v", dayOfMonth, err)
					} else if day > 0 && day < 32 {
						monthIndex := locale.Month(monthName) + 1

						if monthIndex > 0 {
							datetime := time.Date(int(year), time.Month(monthIndex), int(day), 0, 0, 0, 0, time.UTC)
							comp.numberValue = datetime.Unix()
						}
					}
				} else if dt := _DATE_REGEX_MONTH_YYYY.FindSubmatch(comp.bytesValue); len(dt) == 3 {
					monthName, fullYear := string(dt[1]), string(dt[2])

					if year, err := strconv.ParseInt(fullYear, 10, 16); err != nil {
						return nil, fmt.Errorf("not a year %v: %v", fullYear, err)
					} else {
						monthIndex := locale.Month(monthName) + 1

						if monthIndex > 0 {
							firstDayOfMonth := time.Date(int(year), time.Month(monthIndex), 1, 0, 0, 0, 0, time.UTC)
							comp.numberValue = firstDayOfMonth.Unix()
							comp.offsetValue = firstDayOfMonth.AddDate(0, 1, -1).Unix() - comp.numberValue
						}
					}

				} else if dt := _DATE_REGEX_DD_MM_YYYY.FindSubmatch(comp.bytesValue); len(dt) == 4 {
					dayOfMonth, monthOfYear, fullYear := string(dt[1]), string(dt[2]), string(dt[3])

					if year, err := strconv.ParseInt(fullYear, 10, 16); err != nil {
						return nil, fmt.Errorf("not a year %v: %v", fullYear, err)
					} else if month, err := strconv.ParseInt(monthOfYear, 10, 8); err != nil {
						return nil, fmt.Errorf("not a month %v: %v", monthOfYear, err)
					} else if day, err := strconv.ParseInt(dayOfMonth, 10, 8); err != nil {
						return nil, fmt.Errorf("not a day %v: %v", dayOfMonth, err)
					} else if day >= 1 && day <= 31 && month >= 1 && month <= 12 {
						datetime := time.Date(int(year), time.Month(month), int(day), 0, 0, 0, 0, time.UTC)
						comp.numberValue = datetime.Unix()
					}
				} else if dt := _DATE_REGEX_YYYY_MM_DD.FindSubmatch(comp.bytesValue); len(dt) == 4 {
					fullYear, monthOfYear, dayOfMonth := string(dt[1]), string(dt[2]), string(dt[3])

					if year, err := strconv.ParseInt(fullYear, 10, 16); err != nil {
						return nil, fmt.Errorf("not a year %v: %v", fullYear, err)
					} else if month, err := strconv.ParseInt(monthOfYear, 10, 8); err != nil {
						return nil, fmt.Errorf("not a month %v: %v", monthOfYear, err)
					} else if day, err := strconv.ParseInt(dayOfMonth, 10, 8); err != nil {
						return nil, fmt.Errorf("not a day %v: %v", dayOfMonth, err)
					} else if day >= 1 && day <= 31 && month >= 1 && month <= 12 {
						datetime := time.Date(int(year), time.Month(month), int(day), 0, 0, 0, 0, time.UTC)
						comp.numberValue = datetime.Unix()
					}
				} else {
					var maybeMonthName = string(comp.bytesValue)

					if monthIndex := locale.Month(maybeMonthName); monthIndex > -1 {
						currentMonthIndex := time.Now().Month()
						currentYear := time.Now().Year()
						month := monthIndex + 1 // golang starts at 1

						if month > int(currentMonthIndex) {
							currentYear -= 1 // if month is in the future, use last year
						}

						firstDayOfMonth := time.Date(currentYear, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
						comp.numberValue = firstDayOfMonth.Unix()
						comp.offsetValue = firstDayOfMonth.AddDate(0, 1, -1).Unix() - comp.numberValue
					} else if len(comp.bytesValue) == 4 { // maybe it's just an year
						if year, err := strconv.ParseInt(string(comp.bytesValue), 10, 16); err == nil {
							currentYear := time.Now().Year()
							if _MIN_YEAR < year && year <= int64(currentYear) {
								firstDayOfYear := time.Date(int(year), time.January, 1, 0, 0, 0, 0, time.UTC)
								lastDayOfYear := time.Date(int(year), time.December, 31, 0, 0, 0, 0, time.UTC)
								comp.numberValue = firstDayOfYear.Unix()
								comp.offsetValue = lastDayOfYear.Unix() - comp.numberValue
							}
						}
					}
				}
			case HEADER_S_SUM: // it can be 10 as in 10,00 RON or 10,50 RON
				var sumText, maxText string

				if bytes.Contains(comp.bytesValue, []byte(",")) {
					sumText = string(bytes.ReplaceAll(value, []byte(","), []byte("")))
				} else {
					sumText = string(comp.bytesValue) + "00" // add remaining 2 decimals
					maxText = string(comp.bytesValue) + "99" // max digits value
				}

				if sum, err := strconv.ParseInt(sumText, 10, 64); err != nil {
					return nil, fmt.Errorf("not an amount %v: %v", sumText, err)
				} else {
					if maxText != "" {
						if max, err := strconv.ParseInt(maxText, 10, 64); err != nil {
							return nil, fmt.Errorf("not an amount %v: %v", maxText, err)
						} else {
							comp.offsetValue = max - sum
						}
					}

					comp.numberValue = sum
				}
			case HEADER_0_BALANCE:
				value := string(comp.bytesValue)
				if val, err := strconv.ParseInt(value, 10, 32); err != nil {
					return nil, fmt.Errorf("not a number %v: %v", value, err)
				} else {
					comp.numberValue = val // mostly used to compare against 0, "is it positive or negative?" wrt balance
				}
			}
		}

		filters = append(filters, comp)
	}

	return filters, nil
}

func query(records Collection, filters []comparator) (Collection, error) {
	if len(records) == 0 || len(filters) == 0 {
		return records, nil
	}

	var newRecords = make([]Record, 0)
	for _, record := range records {
		if ok, err := filters[0].Compare(record); err != nil {
			return nil, err
		} else if ok {
			newRecords = append(newRecords, record)
		}

	}

	return query(newRecords, filters[1:])
}
