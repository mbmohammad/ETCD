package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type Client struct {
	BaseURL string
	http    *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL: baseURL,
		http:    &http.Client{},
	}
}

func (c *Client) CheckConnection() error {
	resp, err := c.http.Get(c.BaseURL + "/ping")
	if err != nil {
		return fmt.Errorf("connection error: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ping failed: %s", resp.Status)
	}
	return nil
}

func (c *Client) AddPartitions(count int) error {
	body, err := json.Marshal(map[string]int{"count": count})
	if err != nil {
		return fmt.Errorf("add partitions failed: %w", err)
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/partitions/add", c.BaseURL), bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("add partitions failed: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("add partitions failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("add partitions failed: %s - %s", resp.Status, string(bodyBytes))
	}

	return nil
}

func (c *Client) Set(key, value string) error {
	escapedKey := url.PathEscape(key)
	escapedValue := url.PathEscape(value)
	url := fmt.Sprintf("%s/client/%s/%s", c.BaseURL, escapedKey, escapedValue)

	resp, err := c.http.Post(url, "text/plain", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("set failed: %s", resp.Status)
	}
	return nil
}

func (c *Client) Get(key string) (string, error) {
	escapedKey := url.PathEscape(key)
	url := fmt.Sprintf("%s/client/%s", c.BaseURL, escapedKey)

	resp, err := c.http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("get failed: %s", resp.Status)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (c *Client) Delete(key string) error {
	escapedKey := url.PathEscape(key)
	url := fmt.Sprintf("%s/client/%s", c.BaseURL, escapedKey)

	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("delete failed: %s", resp.Status)
	}
	return nil
}
