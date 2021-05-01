package assert

import (
	"errors"
	"testing"
)

// NOTES:
// - Run "go test" to run tests
// - Run "gocov test | gocov report" to report on test converage by file
// - Run "gocov test | gocov annotate -" to report on all code and functions, those ,marked with "MISS" were never called
//
// or
//
// -- may be a good idea to change to output path to somewherelike /tmp
// go test -coverprofile cover.out && go tool cover -html=cover.out -o cover.html
//

func MyCustomErrorHandler(t *testing.T, errs map[string]string, key, expected string) {
	val, ok := errs[key]
	EqualSkip(t, 2, ok, true)
	NotEqualSkip(t, 2, val, nil)
	EqualSkip(t, 2, val, expected)
}

func TestRegexMatchAndNotMatch(t *testing.T) {
	goodRegex := "^(.*/vendor/)?github.com/go-playground/assert$"

	MatchRegex(t, "github.com/go-playground/assert", goodRegex)
	MatchRegex(t, "/vendor/github.com/go-playground/assert", goodRegex)

	NotMatchRegex(t, "/vendor/github.com/go-playground/test", goodRegex)
}

func TestBasicAllGood(t *testing.T) {

	err := errors.New("my error")
	NotEqual(t, err, nil)
	Equal(t, err.Error(), "my error")

	err = nil
	Equal(t, err, nil)

	fn := func() {
		panic("omg omg omg!")
	}

	PanicMatches(t, func() { fn() }, "omg omg omg!")
	PanicMatches(t, func() { panic("omg omg omg!") }, "omg omg omg!")

	/* if you uncomment creates hard fail, that is expected
	// you cant really do this, but it is here for the sake of completeness
	fun := func() {}
	PanicMatches(t, func() { fun() }, "omg omg omg!")
	*/
	errs := map[string]string{}
	errs["Name"] = "User Name Invalid"
	errs["Email"] = "User Email Invalid"

	MyCustomErrorHandler(t, errs, "Name", "User Name Invalid")
	MyCustomErrorHandler(t, errs, "Email", "User Email Invalid")
}

func TestEquals(t *testing.T) {

	type Test struct {
		Name string
	}

	tst := &Test{
		Name: "joeybloggs",
	}

	Equal(t, tst, tst)

	NotEqual(t, tst, nil)
	NotEqual(t, nil, tst)

	type TestMap map[string]string

	var tm TestMap

	Equal(t, tm, nil)
	Equal(t, nil, tm)

	var iface interface{}
	var iface2 interface{}

	iface = 1
	Equal(t, iface, 1)
	NotEqual(t, iface, iface2)
}
