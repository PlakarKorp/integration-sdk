package inventory

import "strings"

// Country is where a resource physically sits, as an ISO 3166-1 alpha-2 code.
// It is an observation, not a rule: whether data is allowed to be there is the
// control plane's question, not the integration's.
//
// Empty means we cannot tell -- correct for an SFTP host or a local filesystem.
// It never means "same as the inventory": nothing is inferred from anything else.
type Country string

// CountryOf normalizes a code, returning "" for anything that is not one. Use it
// on the way out so an unmapped region becomes "unknown" rather than a bad code.
//
// The shape is checked rather than the code looked up in a list of countries:
// that list changes, and an integration somewhere we have not heard of should not
// wait for an SDK release.
func CountryOf(code string) Country {
	code = strings.ToUpper(strings.TrimSpace(code))
	if len(code) != 2 {
		return ""
	}
	for i := 0; i < len(code); i++ {
		if code[i] < 'A' || code[i] > 'Z' {
			return ""
		}
	}
	return Country(code)
}
