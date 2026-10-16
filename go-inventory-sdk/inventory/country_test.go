package inventory

import "testing"

func TestCountryOf(t *testing.T) {
	t.Run("accepts a country code and upcases it", func(t *testing.T) {
		for _, in := range []string{"FR", "fr", " fr ", "Fr"} {
			if got := CountryOf(in); got != "FR" {
				t.Fatalf("CountryOf(%q) = %q, want FR", in, got)
			}
		}
	})

	t.Run("anything else means cannot tell", func(t *testing.T) {
		// The four ways this has been wrong in practice: a region string, a
		// country name, an alpha-3 code, and nothing at all.
		for _, in := range []string{"", "fr-par", "FRANCE", "FRA", "F", "F1", "eu-west-3"} {
			if got := CountryOf(in); got != "" {
				t.Fatalf("CountryOf(%q) = %q, want empty", in, got)
			}
		}
	})
}
