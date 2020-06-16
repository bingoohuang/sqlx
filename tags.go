package sqlx

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

var (
	// ErrTagSyntax ...
	ErrTagSyntax = errors.New("bad syntax for struct tag pair")
	// ErrTagKeySyntax ...
	ErrTagKeySyntax = errors.New("bad syntax for struct tag key")
	// ErrTagValueSyntax ...
	ErrTagValueSyntax = errors.New("bad syntax for struct tag value")
)

// Tags represent a set of tags from a single struct field.
type Tags map[string]Tag

// Tag defines a single struct's string literal tag.
type Tag struct {
	// Key is the tag key, such as json, xml, etc..
	// i.e: `json:"foo,omitempty". Here key is: "json"
	Key string

	// Name is a part of the value
	// i.e: `json:"foo,omitempty". Here value is: "foo,omitempty"
	Value string
}

// ParseTags parses a single struct field tag and returns the set of tags.
// nolint:funlen,gocognit
func ParseTags(tag string) (Tags, error) {
	tags := make(map[string]Tag)

	// NOTE(arslan) following code is from reflect and vet package with some
	// modifications to collect all necessary information and extend it with
	// usable methods
	for tag != "" {
		// Skip leading space.
		i := 0
		for i < len(tag) && tag[i] == ' ' {
			i++
		}

		tag = tag[i:]

		if tag == "" {
			break
		}

		// Scan to colon. A space, a quote or a control character is a syntax
		// error. Strictly speaking, control chars include the range [0x7f,
		// 0x9f], not just [0x00, 0x1f], but in practice, we ignore the
		// multi-byte control characters as it is simpler to inspect the tag's
		// bytes than the tag's runes.
		i = 0
		for i < len(tag) && tag[i] > ' ' && tag[i] != ':' && tag[i] != '"' && tag[i] != 0x7f {
			i++
		}

		if i == 0 {
			return nil, ErrTagKeySyntax
		}

		if i+1 >= len(tag) || tag[i] != ':' {
			return nil, ErrTagSyntax
		}

		if tag[i+1] != '"' {
			return nil, ErrTagValueSyntax
		}

		key := tag[:i]
		tag = tag[i+1:]

		// Scan quoted string to find value.
		i = 1
		for i < len(tag) && tag[i] != '"' {
			if tag[i] == '\\' {
				i++
			}
			i++
		}

		if i >= len(tag) {
			return nil, ErrTagValueSyntax
		}

		qvalue := tag[:i+1]
		tag = tag[i+1:]

		value, err := strconv.Unquote(qvalue)
		if err != nil {
			return nil, ErrTagValueSyntax
		}

		value = strings.TrimSpace(value)
		tags[key] = Tag{Key: key, Value: value}
	}

	return tags, nil
}

// Get returns the tag associated with the given key. If the key is present
// in the tag the value (which may be empty) is returned. Otherwise the
// returned value will be the empty string. The ok return value reports whether
// the tag exists or not (which the return value is nil).
func (t Tags) Get(key string) string { return t[key].Value }

// Keys returns a sorted slice of tag keys.
func (t Tags) Keys() []string {
	keys := make([]string, 0, len(t))

	for key := range t {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys
}

// String reassembles the tags into a valid literal tag field representation.
func (t Tags) String() string {
	tags := t
	if len(tags) == 0 {
		return ""
	}

	keys := t.Keys()
	sort.Strings(keys)

	var buf bytes.Buffer

	for i := 0; i < len(keys); i++ {
		tag := t[keys[i]]

		buf.WriteString(tag.String())

		if i < len(keys)-1 {
			buf.WriteString(" ")
		}
	}

	return buf.String()
}

// Map convert tags to map[string]string.
func (t Tags) Map() map[string]string {
	m := make(map[string]string)

	for k, v := range t {
		m[k] = v.Value
	}

	return m
}

// GetOrDefault gets the value associated to the key
// or defaultValue is return when value is empty.
func (t Tags) GetOrDefault(key, defaultValue string) string {
	if v, ok := t[key]; ok && v.Value != "" {
		return v.Value
	}

	return defaultValue
}

// String reassembles the tag into a valid tag field representation.
func (t Tag) String() string {
	return fmt.Sprintf(`%s:%q`, t.Key, t.Value)
}

// GoString implements the fmt.GoStringer interface.
func (t Tag) GoString() string {
	template := `{
		Key:    '%s',
		Value:   '%s',
	}`

	return fmt.Sprintf(template, t.Key, t.Value)
}
