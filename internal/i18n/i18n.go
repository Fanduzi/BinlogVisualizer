// Package i18n provides internationalization support for binlogviz.
// It loads embedded locale files and provides translation functions.
//
// Note: This package uses global state and is designed for CLI usage where
// initialization happens once at startup. Thread safety is ensured via sync.RWMutex.
// Command descriptions (cobra Use/Short fields) are evaluated at package init time
// before language is determined, so they will always appear in English.
// Runtime output (errors, reports, progress) is properly localized.
package i18n

import (
	"embed"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/nicksnyder/go-i18n/v2/i18n"
	"golang.org/x/text/language"
)

//go:embed locales/*.json
var localeFS embed.FS

// bundle holds all loaded translations.
var bundle *i18n.Bundle

// localizer is the current active localizer.
var localizer *i18n.Localizer

// mu protects localizer from concurrent read/write access.
var mu sync.RWMutex

// initOnce ensures initialization happens exactly once.
var initOnce sync.Once

// initErr stores any error from initialization.
var initErr error

// Init initializes the i18n system with the given language.
// If lang is empty, it detects language from environment variables.
// Supported languages: en (default), zh-CN.
func Init(lang string) error {
	initOnce.Do(func() {
		bundle = i18n.NewBundle(language.English)
		bundle.RegisterUnmarshalFunc("json", json.Unmarshal)

		// Load embedded locale files
		localeFiles := []string{"locales/en.json", "locales/zh-CN.json"}
		for _, file := range localeFiles {
			_, err := bundle.LoadMessageFileFS(localeFS, file)
			if err != nil {
				initErr = fmt.Errorf("load locale file %s: %w", file, err)
				return
			}
		}

		// Create localizer with initial language (may be updated later)
		mu.Lock()
		localizer = i18n.NewLocalizer(bundle, "en")
		mu.Unlock()
	})

	// Always update localizer if bundle is ready
	if initErr == nil && bundle != nil {
		if lang == "" {
			lang = detectLanguageFromEnv()
		}
		mu.Lock()
		localizer = i18n.NewLocalizer(bundle, lang)
		mu.Unlock()
	}

	return initErr
}

// detectLanguageFromEnv detects language from LANG or LC_ALL environment variables.
// Returns "zh-CN" for Chinese locales, otherwise defaults to "en".
func detectLanguageFromEnv() string {
	// Check LC_ALL first
	if lcAll := os.Getenv("LC_ALL"); lcAll != "" {
		if isChineseLocale(lcAll) {
			return "zh-CN"
		}
	}

	// Check LANG
	if lang := os.Getenv("LANG"); lang != "" {
		if isChineseLocale(lang) {
			return "zh-CN"
		}
	}

	// Default to English
	return "en"
}

// isChineseLocale checks if a locale string represents Chinese.
func isChineseLocale(locale string) bool {
	lower := strings.ToLower(locale)
	return strings.HasPrefix(lower, "zh") ||
		strings.Contains(lower, "zh_cn") ||
		strings.Contains(lower, "zh-cn") ||
		strings.Contains(lower, "zh_tw") ||
		strings.Contains(lower, "zh-tw")
}

// T translates a message by key with optional template data.
// If translation is not found, returns the key itself.
// If i18n is not initialized, auto-initializes with English.
func T(key string, templateData ...map[string]any) string {
	// Auto-initialize with English if not already initialized
	mu.RLock()
	loc := localizer
	mu.RUnlock()

	if loc == nil {
		if err := Init("en"); err != nil {
			// Fallback to key on initialization failure
			return key
		}
		mu.RLock()
		loc = localizer
		mu.RUnlock()
	}

	var data map[string]any
	if len(templateData) > 0 {
		data = templateData[0]
	}

	msg, err := loc.Localize(&i18n.LocalizeConfig{
		MessageID:    key,
		TemplateData: data,
	})
	if err != nil {
		return key
	}
	return msg
}

// Tf is an alias for T with template data, for explicit clarity.
// Use T(key, data) directly for the same effect.
func Tf(key string, data map[string]any) string {
	return T(key, data)
}

// MustInit initializes i18n and panics on error.
// Use this in tests or when initialization must succeed.
func MustInit(lang string) {
	if err := Init(lang); err != nil {
		panic(err)
	}
}

// SetLanguage changes the current language at runtime.
// Returns false if i18n has not been initialized.
func SetLanguage(lang string) bool {
	mu.RLock()
	b := bundle
	mu.RUnlock()

	if b == nil {
		return false
	}
	mu.Lock()
	localizer = i18n.NewLocalizer(b, lang)
	mu.Unlock()
	return true
}

// CurrentLanguage returns the current language tag.
// Returns "en" if i18n is not initialized.
func CurrentLanguage() string {
	mu.RLock()
	loc := localizer
	mu.RUnlock()

	if loc == nil {
		return "en"
	}
	// Get the language from a test translation
	// This is a workaround since go-i18n doesn't expose the current tag directly
	lang, err := loc.Localize(&i18n.LocalizeConfig{
		MessageID: "meta.language",
	})
	if err != nil {
		return "en"
	}
	return lang
}

// ResetForTesting resets the i18n state for test isolation.
// This function is intended for testing purposes only.
func ResetForTesting() {
	mu.Lock()
	defer mu.Unlock()
	bundle = nil
	localizer = nil
	initOnce = sync.Once{}
	initErr = nil
}
