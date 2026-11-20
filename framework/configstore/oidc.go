package configstore

import (
	"encoding/json"
	"fmt"
	"net"
	"net/mail"
	"net/url"
	"reflect"
	"slices"
	"strings"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/encrypt"
)

// OIDCConfig uses one issuer to authenticate explicitly allowed dashboard administrators.
// Inference continues to use its existing virtual-key policy.
type OIDCConfig struct {
	Enabled         bool               `json:"enabled"`
	IssuerURL       string             `json:"issuer_url"`
	ClientID        string             `json:"client_id"`
	ClientSecret    *schemas.SecretVar `json:"client_secret,omitempty"`
	RedirectURL     string             `json:"redirect_url"`
	AllowedSubjects []string           `json:"allowed_subjects,omitempty"`
	AllowedEmails   []string           `json:"allowed_emails,omitempty"`
}

func (c *OIDCConfig) Clone() *OIDCConfig {
	if c == nil {
		return nil
	}
	copy := *c
	copy.ClientSecret = c.ClientSecret.Clone()
	copy.AllowedSubjects = slices.Clone(c.AllowedSubjects)
	copy.AllowedEmails = slices.Clone(c.AllowedEmails)
	return &copy
}

func (c *OIDCConfig) Redacted() *OIDCConfig {
	copy := c.Clone()
	if copy != nil {
		copy.ClientSecret = copy.ClientSecret.FullyRedacted()
	}
	return copy
}

func (c *OIDCConfig) Equals(other *OIDCConfig) bool {
	return reflect.DeepEqual(c, other)
}

func (c *OIDCConfig) Validate() error {
	if c == nil || !c.Enabled {
		return nil
	}
	for field, value := range map[string]string{"issuer_url": c.IssuerURL, "redirect_url": c.RedirectURL} {
		u, err := url.Parse(value)
		if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return fmt.Errorf("oidc.%s must be an absolute URL without credentials, query or fragment", field)
		}
		ip := net.ParseIP(u.Hostname())
		loopback := strings.EqualFold(u.Hostname(), "localhost") || (ip != nil && ip.IsLoopback())
		if u.Scheme != "https" && !(u.Scheme == "http" && loopback) {
			return fmt.Errorf("oidc.%s requires HTTPS (HTTP is allowed for loopback development)", field)
		}
		if field == "redirect_url" && !strings.HasSuffix(u.Path, "/api/session/oidc/callback") {
			return fmt.Errorf("oidc.redirect_url must end in /api/session/oidc/callback")
		}
	}
	if strings.TrimSpace(c.ClientID) == "" {
		return fmt.Errorf("oidc.client_id is required")
	}
	if c.ClientSecret.IsFromSecret() && c.ClientSecret.GetValue() == "" {
		return fmt.Errorf("oidc.client_secret reference resolved to an empty value")
	}
	if len(c.AllowedSubjects)+len(c.AllowedEmails) == 0 {
		return fmt.Errorf("oidc requires at least one allowed subject or verified email")
	}
	for _, subject := range c.AllowedSubjects {
		if strings.TrimSpace(subject) == "" {
			return fmt.Errorf("oidc.allowed_subjects must not contain empty values")
		}
	}
	for _, email := range c.AllowedEmails {
		parsed, err := mail.ParseAddress(email)
		if err != nil || parsed.Address != email {
			return fmt.Errorf("oidc.allowed_emails must contain email addresses")
		}
	}
	return nil
}

func (c *OIDCConfig) Allows(subject, email string, verified bool) bool {
	if c == nil || subject == "" {
		return false
	}
	if slices.Contains(c.AllowedSubjects, subject) {
		return true
	}
	return verified && slices.ContainsFunc(c.AllowedEmails, func(allowed string) bool { return strings.EqualFold(allowed, email) })
}

func (c *AuthConfig) OIDCEnabled() bool {
	return c != nil && c.IsEnabled && c.OIDC != nil && c.OIDC.Enabled
}

func (c *AuthConfig) HasPassword() bool {
	return c != nil && c.AdminUserName.GetValue() != "" && c.AdminPassword.GetValue() != ""
}

// Equals compares the persisted credentials and dashboard access policy.
func (c *AuthConfig) Equals(other *AuthConfig) bool {
	if c == nil || other == nil {
		return c == other
	}
	return c.IsEnabled == other.IsEnabled && c.AdminUserName.GetValue() == other.AdminUserName.GetValue() &&
		c.AdminPassword.GetValue() == other.AdminPassword.GetValue() && c.OIDC.Equals(other.OIDC)
}

// The existing governance-config key/value store keeps OIDC settings together.
// Secret references retain only their reference; literal credentials use the
// same at-rest encryption key as the rest of Bifrost's config store.
type storedOIDCConfig struct {
	Value     string `json:"value"`
	Encrypted bool   `json:"encrypted,omitempty"`
}

func encodeOIDCConfig(config *OIDCConfig) (string, error) {
	if config == nil {
		return "", nil
	}
	copy := config.Clone()
	if copy.ClientSecret.IsFromSecret() {
		copy.ClientSecret.Val = ""
	}
	data, err := json.Marshal(copy)
	if err != nil {
		return "", err
	}
	value, err := encrypt.Encrypt(string(data))
	if err != nil {
		return "", err
	}
	data, err = json.Marshal(storedOIDCConfig{Value: value, Encrypted: encrypt.IsEnabled()})
	return string(data), err
}

func decodeOIDCConfig(data string) (*OIDCConfig, error) {
	if data == "" {
		return nil, nil
	}
	var stored storedOIDCConfig
	if err := json.Unmarshal([]byte(data), &stored); err != nil {
		return nil, err
	}
	if stored.Encrypted {
		value, err := encrypt.Decrypt(stored.Value)
		if err != nil {
			return nil, err
		}
		stored.Value = value
	}
	var config OIDCConfig
	if err := json.Unmarshal([]byte(stored.Value), &config); err != nil {
		return nil, err
	}
	return &config, nil
}
