package configstore

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/mail"
	"net/url"
	"slices"
	"strings"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/maximhq/bifrost/framework/encrypt"
	"gorm.io/gorm"
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
	if c == nil || other == nil {
		return c == other
	}
	return c.Enabled == other.Enabled && c.IssuerURL == other.IssuerURL && c.ClientID == other.ClientID &&
		c.ClientSecret.Equals(other.ClientSecret) && c.RedirectURL == other.RedirectURL &&
		equalOIDCAllowlist(c.AllowedSubjects, other.AllowedSubjects, func(a, b string) bool { return a == b }) &&
		equalOIDCAllowlist(c.AllowedEmails, other.AllowedEmails, strings.EqualFold)
}

// Match Allows' membership rules: ordering, duplicates, and email case do not
// change access and must not revoke sessions or invalidate an in-flight login.
func equalOIDCAllowlist(a, b []string, equal func(string, string) bool) bool {
	containsAll := func(values, allowed []string) bool {
		for _, value := range values {
			if !slices.ContainsFunc(allowed, func(other string) bool { return equal(value, other) }) {
				return false
			}
		}
		return true
	}
	return containsAll(a, b) && containsAll(b, a)
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

// Encrypt the existing envelope directly, without resolving references or
// changing the auth policy. Reopening an encrypted store is a no-op.
func (s *RDBConfigStore) encryptPlaintextOIDCConfig(ctx context.Context) (int, error) {
	count := 0
	err := s.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var rows []tables.TableGovernanceConfig
		if err := lockRow(tx).Where("key = ?", tables.ConfigOIDCKey).Find(&rows).Error; err != nil {
			return err
		}
		if len(rows) == 0 || rows[0].Value == "" {
			return nil
		}
		var stored storedOIDCConfig
		if err := json.Unmarshal([]byte(rows[0].Value), &stored); err != nil {
			return err
		}
		if stored.Encrypted {
			return nil
		}
		value, err := encrypt.Encrypt(stored.Value)
		if err != nil {
			return err
		}
		stored.Value, stored.Encrypted = value, true
		data, err := json.Marshal(stored)
		if err != nil {
			return err
		}
		if err := tx.Model(&rows[0]).Update("value", string(data)).Error; err != nil {
			return err
		}
		count = 1
		return nil
	})
	return count, err
}
