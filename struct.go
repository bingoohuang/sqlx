package sqlx

import "reflect"

// StructField represents the information of a struct's field.
type StructField struct {
	Parent      *StructValue
	Field       reflect.Value
	Index       int
	StructField reflect.StructField
	Type        reflect.Type
	Name        string
	Tag         reflect.StructTag
	Kind        reflect.Kind
	PkgPath     string
}

// GetTag returns the value associated with key in the tag string.
// If there is no such key in the tag, Get returns the empty string.
func (f StructField) GetTag(name string) string {
	if v, ok := f.Tag.Lookup(name); ok {
		return v
	}

	return ""
}

// GetTagOr returns the tag's value of the field or defaultValue when tag is empty.
func (f StructField) GetTagOr(tagName string, defaultValue string) string {
	tag := f.GetTag(tagName)
	if tag != "" {
		return tag
	}

	return defaultValue
}

// StructValue represents the structure for a struct.
type StructValue struct {
	StructSelf reflect.Value
	NumField   int
	FieldTypes []reflect.StructField
}

// MakeStructValue makes a StructValue by a struct's value.
func MakeStructValue(structSelf reflect.Value) *StructValue {
	sv := &StructValue{StructSelf: structSelf, NumField: structSelf.NumField()}

	sv.FieldTypes = make([]reflect.StructField, sv.NumField)
	for i := 0; i < sv.NumField; i++ {
		sv.FieldTypes[i] = sv.StructSelf.Type().Field(i)
	}

	return sv
}

// FieldIndexByName return's the index of field by its name.
// If the field is not found, FieldIndexByName returns -1.
func (s *StructValue) FieldIndexByName(name string) int {
	for i, ft := range s.FieldTypes {
		if ft.Name == name {
			return i
		}
	}

	return -1
}

// FieldByName returns the StructField which has the name.
func (s *StructValue) FieldByName(name string) (StructField, bool) {
	index := s.FieldIndexByName(name)
	if index < 0 {
		return StructField{}, false
	}

	return s.FieldByIndex(index), true
}

// FieldByIndex return the StructField at index.
func (s *StructValue) FieldByIndex(index int) StructField {
	fieldType := s.FieldTypes[index]
	field := s.StructSelf.Field(index)

	return StructField{
		Parent:      s,
		Field:       field,
		Index:       index,
		StructField: fieldType,
		Type:        fieldType.Type,
		Name:        fieldType.Name,
		Tag:         fieldType.Tag,
		Kind:        field.Kind(),
		PkgPath:     fieldType.PkgPath,
	}
}
