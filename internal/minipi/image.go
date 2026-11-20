package minipi

import (
	"errors"
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Image is a remote user attachment. Label associates it with the quoted event
// or forwarded element in Content. Neither field may be written to logs.
// Only the model provider fetches URL; persistence does not cache image bytes.
type Image struct {
	URL   string `json:"url"`
	Label string `json:"label,omitempty"`
}

// ValidateImages is shared by the agent, durable input and protocol adapters.
// Errors deliberately exclude URLs and labels. Providers enforce their own
// image count, format, size and remote access restrictions.
func ValidateImages(role Role, images []Image) error {
	bad := errors.New("minipi: invalid image input")
	if len(images) > 0 && role != RoleUser {
		return bad
	}
	for _, image := range images {
		if !utf8.ValidString(image.URL) || !utf8.ValidString(image.Label) || strings.ContainsFunc(image.URL, func(r rune) bool {
			return unicode.IsSpace(r) || unicode.IsControl(r)
		}) {
			return bad
		}
		u, err := url.Parse(image.URL)
		if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Hostname() == "" || u.User != nil || u.Fragment != "" || u.Opaque != "" {
			return bad
		}
	}
	return nil
}
