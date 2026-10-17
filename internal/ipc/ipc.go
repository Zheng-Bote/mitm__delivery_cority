/**
 * SPDX-FileComment: Scheduler IPC Client
 * SPDX-FileType: SOURCE
 * SPDX-FileContributor: ZHENG Robert
 * SPDX-FileCopyrightText: 2026 ZHENG Robert
 * SPDX-License-Identifier: Apache-2.0
 *
 * @file ipc.go
 * @brief Nil-safe status and audit events to the scheduler over one persistent Unix socket connection
 * @version 0.22.0
 * @date 2026-10-17
 *
 * @author ZHENG Robert (robert@hase-zheng.net)
 * @copyright Copyright (c) 2026 ZHENG Robert
 * @LICENSE Apache-2.0
 */

package ipc

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
)

type StatusEvent struct {
	RunID     int    `json:"run_id"`
	Type      string `json:"type"` // "status" (default) or "audit"
	Component string `json:"component"`
	Status    string `json:"status"`
	Message   string `json:"message"`
	Progress  int    `json:"progress"`
}

type IPCClient struct {
	SocketPath string
	RunID      int
	Component  string
	Topic      string
	SourceName string

	mu   sync.Mutex
	conn net.Conn
}

func (c *IPCClient) SendEvent(status, message string, progress int) {
	if c == nil || c.SocketPath == "" {
		return
	}
	c.send(StatusEvent{
		RunID:     c.RunID,
		Type:      "status",
		Component: c.Component,
		Status:    status,
		Message:   c.prefix(message),
		Progress:  progress,
	})
}

func (c *IPCClient) SendAudit(message string) {
	if c == nil || c.SocketPath == "" {
		return
	}
	c.send(StatusEvent{
		RunID:     c.RunID,
		Type:      "audit",
		Component: c.Component,
		Message:   c.prefix(message),
	})
}

// Close closes the persistent connection; later events re-dial.
func (c *IPCClient) Close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

func (c *IPCClient) prefix(message string) string {
	if c.Topic != "" && c.SourceName != "" {
		return fmt.Sprintf("%s: %s: %s", c.Topic, c.SourceName, message)
	}
	return message
}

// send writes one event line over the persistent connection, re-dialling once on failure.
// Events are best-effort: errors are dropped so that IPC problems never stop a delivery.
func (c *IPCClient) send(event StatusEvent) {
	data, _ := json.Marshal(event)
	data = append(data, '\n')

	c.mu.Lock()
	defer c.mu.Unlock()
	for attempt := 0; attempt < 2; attempt++ {
		if c.conn == nil {
			conn, err := net.Dial("unix", c.SocketPath)
			if err != nil {
				return
			}
			c.conn = conn
		}
		if _, err := c.conn.Write(data); err == nil {
			return
		}
		c.conn.Close()
		c.conn = nil
	}
}
