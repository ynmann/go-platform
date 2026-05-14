package telegram

import "time"

// Config configures the Telegram Bot API client.
//
// IsEnabled is the master switch — when false New returns a no-op client
// that silently drops all calls. BotToken is expected to come from Vault
// (key TELEGRAM_BOT_TOKEN); the YAML value is a placeholder and should
// stay empty in committed configs.
type Config struct {
	IsEnabled bool `yaml:"is_enabled" json:"is_enabled" mapstructure:"is_enabled"`

	BotToken      string `yaml:"bot_token"       json:"bot_token"       mapstructure:"bot_token"       vault:"TELEGRAM_BOT_TOKEN"`
	DefaultChatId string `yaml:"default_chat_id" json:"default_chat_id" mapstructure:"default_chat_id" vault:"TELEGRAM_DEFAULT_CHAT_ID"`

	ApiBaseURL           string        `yaml:"api_base_url"          json:"api_base_url"          mapstructure:"api_base_url"`
	ParseMode            string        `yaml:"parse_mode"            json:"parse_mode"            mapstructure:"parse_mode"`
	Timeout              time.Duration `yaml:"timeout"               json:"timeout"               mapstructure:"timeout"`
	DisableNotifications bool          `yaml:"disable_notifications" json:"disable_notifications" mapstructure:"disable_notifications"`
}

const (
	defaultApiBaseURL = "https://api.telegram.org"
	defaultTimeout    = 10 * time.Second
)
