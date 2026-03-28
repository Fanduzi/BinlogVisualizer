package i18n

import (
	"os"
	"sync"
	"testing"
)

func TestInit(t *testing.T) {
	tests := []struct {
		name    string
		lang    string
		wantErr bool
	}{
		{
			name:    "initialize with English",
			lang:    "en",
			wantErr: false,
		},
		{
			name:    "initialize with Chinese",
			lang:    "zh-CN",
			wantErr: false,
		},
		{
			name:    "initialize with empty language (detect from env)",
			lang:    "",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ResetForTesting()
			err := Init(tt.lang)
			if (err != nil) != tt.wantErr {
				t.Errorf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestT(t *testing.T) {
	ResetForTesting()
	MustInit("en")

	tests := []struct {
		name     string
		key      string
		data     map[string]any
		expected string
	}{
		{
			name:     "simple translation",
			key:      "cmd.root.short",
			data:     nil,
			expected: "Analyze MySQL binlog files",
		},
		{
			name:     "translation with template data",
			key:      "error.fileNotFound",
			data:     map[string]any{"Path": "/tmp/test.binlog"},
			expected: "file not found: /tmp/test.binlog",
		},
		{
			name:     "missing key returns key itself",
			key:      "nonexistent.key",
			data:     nil,
			expected: "nonexistent.key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := T(tt.key, tt.data)
			if got != tt.expected {
				t.Errorf("T() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestChineseTranslation(t *testing.T) {
	ResetForTesting()
	MustInit("zh-CN")

	tests := []struct {
		name     string
		key      string
		data     map[string]any
		expected string
	}{
		{
			name:     "simple Chinese translation",
			key:      "cmd.root.short",
			data:     nil,
			expected: "分析 MySQL binlog 文件",
		},
		{
			name:     "Chinese translation with template data",
			key:      "error.fileNotFound",
			data:     map[string]any{"Path": "/tmp/test.binlog"},
			expected: "文件未找到：/tmp/test.binlog",
		},
		{
			name:     "report section header",
			key:      "report.section.workload",
			data:     nil,
			expected: "工作负载摘要",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := T(tt.key, tt.data)
			if got != tt.expected {
				t.Errorf("T() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestDetectLanguageFromEnv(t *testing.T) {
	tests := []struct {
		name     string
		langEnv  string
		lcAllEnv string
		expected string
	}{
		{
			name:     "no env vars - default to English",
			langEnv:  "",
			lcAllEnv: "",
			expected: "en",
		},
		{
			name:     "LANG set to Chinese",
			langEnv:  "zh_CN.UTF-8",
			lcAllEnv: "",
			expected: "zh-CN",
		},
		{
			name:     "LANG set to zh_CN",
			langEnv:  "zh_CN",
			lcAllEnv: "",
			expected: "zh-CN",
		},
		{
			name:     "LANG set to zh-TW",
			langEnv:  "zh_TW.UTF-8",
			lcAllEnv: "",
			expected: "zh-CN",
		},
		{
			name:     "LANG set to English",
			langEnv:  "en_US.UTF-8",
			lcAllEnv: "",
			expected: "en",
		},
		{
			name:     "LC_ALL takes precedence over LANG",
			langEnv:  "en_US.UTF-8",
			lcAllEnv: "zh_CN.UTF-8",
			expected: "zh-CN",
		},
		{
			name:     "LC_ALL set to English",
			langEnv:  "",
			lcAllEnv: "en_US.UTF-8",
			expected: "en",
		},
	}

	originalLang := os.Getenv("LANG")
	originalLcAll := os.Getenv("LC_ALL")
	defer func() {
		os.Setenv("LANG", originalLang)
		os.Setenv("LC_ALL", originalLcAll)
	}()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("LANG", tt.langEnv)
			os.Setenv("LC_ALL", tt.lcAllEnv)
			got := detectLanguageFromEnv()
			if got != tt.expected {
				t.Errorf("detectLanguageFromEnv() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestSetLanguage(t *testing.T) {
	ResetForTesting()
	// Initialize first
	MustInit("en")

	// Verify English
	if got := T("cmd.root.short"); got != "Analyze MySQL binlog files" {
		t.Errorf("Expected English translation, got: %v", got)
	}

	// Switch to Chinese
	if !SetLanguage("zh-CN") {
		t.Error("SetLanguage should return true when initialized")
	}

	if got := T("cmd.root.short"); got != "分析 MySQL binlog 文件" {
		t.Errorf("Expected Chinese translation, got: %v", got)
	}

	// Switch back to English
	SetLanguage("en")

	if got := T("cmd.root.short"); got != "Analyze MySQL binlog files" {
		t.Errorf("Expected English translation, got: %v", got)
	}
}

func TestIsChineseLocale(t *testing.T) {
	tests := []struct {
		locale   string
		expected bool
	}{
		{"zh_CN.UTF-8", true},
		{"zh_CN", true},
		{"zh-CN", true},
		{"zh_TW.UTF-8", true},
		{"zh_TW", true},
		{"zh-Hans", true},
		{"en_US.UTF-8", false},
		{"en_US", false},
		{"ja_JP.UTF-8", false},
		{"ko_KR.UTF-8", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.locale, func(t *testing.T) {
			got := isChineseLocale(tt.locale)
			if got != tt.expected {
				t.Errorf("isChineseLocale(%q) = %v, want %v", tt.locale, got, tt.expected)
			}
		})
	}
}

func TestTf(t *testing.T) {
	ResetForTesting()
	MustInit("en")

	got := Tf("error.fileNotFound", map[string]any{"Path": "/test/path"})
	expected := "file not found: /test/path"

	if got != expected {
		t.Errorf("Tf() = %v, want %v", got, expected)
	}
}

func TestCurrentLanguage(t *testing.T) {
	ResetForTesting()
	MustInit("en")
	if got := CurrentLanguage(); got != "en" {
		t.Errorf("CurrentLanguage() = %v, want en", got)
	}

	ResetForTesting()
	MustInit("zh-CN")
	if got := CurrentLanguage(); got != "zh-CN" {
		t.Errorf("CurrentLanguage() = %v, want zh-CN", got)
	}
}

func TestSetLanguageReturnsFalseWhenNotInitialized(t *testing.T) {
	ResetForTesting()
	if SetLanguage("en") {
		t.Error("SetLanguage should return false when bundle is nil")
	}
}

func TestConcurrentAccess(t *testing.T) {
	ResetForTesting()
	MustInit("en")

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			T("cmd.root.short")
		}()
		go func() {
			defer wg.Done()
			SetLanguage("en")
		}()
		go func() {
			defer wg.Done()
			CurrentLanguage()
		}()
	}
	wg.Wait()
}
