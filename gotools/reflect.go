package gotools

import (
	"fmt"
	"reflect"
)

// TypeInfo returns a `type=Foo pkg="github.com/x/y"` string describing v.
// Pointers are dereferenced so the printed type matches what the caller
// declares. Useful in panic messages and structured logs where you want the
// element type rather than `*Foo`.
func TypeInfo(v any) string {
	t := reflect.TypeOf(v)
	if t == nil {
		return `type=<nil> pkg=""`
	}
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return fmt.Sprintf("type=%v pkg=%q", t, t.PkgPath())
}
