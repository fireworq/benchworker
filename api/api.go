package api

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

type Client struct {
	url *url.URL
}

func New(urlStr string) (*Client, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	return &Client{u}, nil
}

func (c *Client) CreateQueue(queueName string, maxWorkers, pollingInterval uint) (*http.Response, error) {
	u := *c.url
	u.Path = "/queue/" + url.QueryEscape(queueName)
	return c.put(&u, &Queue{
		MaxWorkers:      maxWorkers,
		PollingInterval: pollingInterval,
	})
}

func (c *Client) CreateRouting(jobCategory, queueName string) (*http.Response, error) {
	u := *c.url
	u.Path = "/routing/" + url.QueryEscape(jobCategory)
	return c.put(&u, &Routing{
		QueueName: queueName,
	})
}

func (c *Client) Enqueue(jobCategory string, job interface{}) (*http.Response, error) {
	u := *c.url
	u.Path = "/job/" + url.QueryEscape(jobCategory)
	return c.post(&u, job)
}

func (c *Client) post(url *url.URL, payload interface{}) (*http.Response, error) {
	return c.write("POST", url, payload)
}

func (c *Client) put(url *url.URL, payload interface{}) (*http.Response, error) {
	return c.write("PUT", url, payload)
}

func (c *Client) write(method string, url *url.URL, payload interface{}) (*http.Response, error) {
	json, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(method, url.String(), bytes.NewReader(json))
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	ioutil.ReadAll(resp.Body)
	return resp, err
}

func init() {
	client = &http.Client{
		Timeout: 10 * time.Second,
	}
}

var client *http.Client

type Queue struct {
	MaxWorkers      uint `json:"max_workers"`
	PollingInterval uint `json:"polling_interval"`
}

type Routing struct {
	QueueName string `json:"queue_name"`
}
