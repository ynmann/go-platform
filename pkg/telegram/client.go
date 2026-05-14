package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/rs/zerolog/log"
)

// Client is the contract every consumer in the codebase should depend on.
// It is safe to use the value returned by New unconditionally — the no-op
// implementation honours all methods when the package is disabled.
type Client interface {
	IsEnabled() bool
	SendMessage(ctx context.Context, chatId, text string) error
	Send(ctx context.Context, msg Message) error
	Close() error
}

// Message represents one outbound notification. ChatId falls back to
// Config.DefaultChatId, ParseMode falls back to Config.ParseMode, and
// DisableNotification, when nil, falls back to Config.DisableNotifications.
type Message struct {
	ChatId              string
	Text                string
	ParseMode           string
	DisableNotification *bool
}

// New returns a Client. When cfg.IsEnabled is false it returns a no-op
// client that does nothing — callers should still consume the returned
// Client. When enabled, New validates configuration and pings Telegram
// (getMe) so misconfiguration surfaces at startup instead of at first
// send.
func New(ctx context.Context, cfg Config) (Client, error) {
	if !cfg.IsEnabled {
		log.Info().Msg("telegram: disabled, using no-op client")
		return noop{}, nil
	}

	if cfg.BotToken == "" {
		return nil, errors.New("telegram: enabled but bot_token is empty (expected from Vault)")
	}

	apiBase := strings.TrimRight(cfg.ApiBaseURL, "/")
	if apiBase == "" {
		apiBase = defaultApiBaseURL
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	c := &httpClient{
		cfg:    cfg,
		http:   &http.Client{Timeout: timeout},
		apiURL: fmt.Sprintf("%s/bot%s", apiBase, cfg.BotToken),
	}

	if err := c.ping(ctx); err != nil {
		return nil, fmt.Errorf("telegram: connect: %w", err)
	}

	log.Info().
		Bool("disable_notifications", cfg.DisableNotifications).
		Dur("timeout", timeout).
		Msg("telegram: connected")

	return c, nil
}

type noop struct{}

func (noop) IsEnabled() bool                                  { return false }
func (noop) SendMessage(_ context.Context, _, _ string) error { return nil }
func (noop) Send(_ context.Context, _ Message) error          { return nil }
func (noop) Close() error                                     { return nil }

type httpClient struct {
	cfg    Config
	http   *http.Client
	apiURL string
}

func (c *httpClient) IsEnabled() bool { return true }

func (c *httpClient) Close() error { return nil }

func (c *httpClient) SendMessage(ctx context.Context, chatId, text string) error {
	return c.Send(ctx, Message{ChatId: chatId, Text: text})
}

type sendMessageRequest struct {
	ChatId              string `json:"chat_id"`
	Text                string `json:"text"`
	ParseMode           string `json:"parse_mode,omitempty"`
	DisableNotification bool   `json:"disable_notification,omitempty"`
}

type telegramResponse struct {
	Ok          bool   `json:"ok"`
	ErrorCode   int    `json:"error_code,omitempty"`
	Description string `json:"description,omitempty"`
}

func (c *httpClient) Send(ctx context.Context, msg Message) error {
	chatId := msg.ChatId
	if chatId == "" {
		chatId = c.cfg.DefaultChatId
	}
	if chatId == "" {
		return errors.New("telegram: empty chat id (no DefaultChatId configured)")
	}

	parseMode := msg.ParseMode
	if parseMode == "" {
		parseMode = c.cfg.ParseMode
	}

	disable := c.cfg.DisableNotifications
	if msg.DisableNotification != nil {
		disable = *msg.DisableNotification
	}

	body, err := json.Marshal(sendMessageRequest{
		ChatId:              chatId,
		Text:                msg.Text,
		ParseMode:           parseMode,
		DisableNotification: disable,
	})
	if err != nil {
		return err
	}

	return c.do(ctx, "sendMessage", body)
}

func (c *httpClient) ping(ctx context.Context) error {
	pingCtx, cancel := context.WithTimeout(ctx, c.http.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(pingCtx, http.MethodGet, c.apiURL+"/getMe", nil)
	if err != nil {
		return err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	rb, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("getMe %d: %s", resp.StatusCode, truncate(rb, 256))
	}

	var tr telegramResponse
	if err := json.Unmarshal(rb, &tr); err == nil && !tr.Ok {
		return fmt.Errorf("getMe not ok: %d %s", tr.ErrorCode, tr.Description)
	}

	return nil
}

func (c *httpClient) do(ctx context.Context, method string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.apiURL+"/"+method, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	rb, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("telegram %s %d: %s", method, resp.StatusCode, truncate(rb, 256))
	}

	var tr telegramResponse
	if err := json.Unmarshal(rb, &tr); err == nil && !tr.Ok {
		return fmt.Errorf("telegram %s not ok: %d %s", method, tr.ErrorCode, tr.Description)
	}

	return nil
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
