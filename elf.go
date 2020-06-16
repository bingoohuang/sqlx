package sqlx

import "unicode"

// FixPkgName fixes the package name to all lower case with letters and digits kept.
func FixPkgName(pkgName string) string {
	name := ""
	started := false

	for _, c := range pkgName {
		if !started {
			started = unicode.IsLetter(c)
		}

		if started {
			if unicode.IsLetter(c) || unicode.IsDigit(c) {
				name += string(unicode.ToLower(c))
			}
		}
	}

	return name
}
