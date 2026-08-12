package config

import (
	"fmt"
	"regexp"
	"strings"
)

var usernameSuffixRegexp = regexp.MustCompile(`@.*$`)

// FixUsername returns the username qualified with the configured user domain, replacing any domain it
// already carries. The DE database stores domain-qualified usernames, so lookups need this form.
func (c *Config) FixUsername(username string) string {
	return fmt.Sprintf("%s@%s", usernameSuffixRegexp.ReplaceAllString(username, ""), c.UserSuffix)
}

// TrimUserSuffix removes the configured user domain from the username, leaving any other
// qualification intact. iRODS usernames may carry a foreign zone that must survive, which is why this
// trims an exact suffix rather than everything after an "@".
func (c *Config) TrimUserSuffix(username string) string {
	return strings.TrimSuffix(username, "@"+c.UserSuffix)
}
