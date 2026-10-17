/**
 * SPDX-FileComment: Scheduler IPC Client Tests
 * SPDX-FileType: SOURCE
 * SPDX-FileContributor: ZHENG Robert
 * SPDX-FileCopyrightText: 2026 ZHENG Robert
 * SPDX-License-Identifier: Apache-2.0
 *
 * @file ipc_test.go
 * @brief Unit tests for connection reuse and reconnect of the IPC client
 * @version 0.22.0
 * @date 2026-10-17
 *
 * @author ZHENG Robert (robert@hase-zheng.net)
 * @copyright Copyright (c) 2026 ZHENG Robert
 * @LICENSE Apache-2.0
 */

package ipc

import (
	"bufio"
	"encoding/json"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// fakeScheduler accepts connections and collects event lines.
type fakeScheduler struct {
	mu      sync.Mutex
	accepts int
	events  []StatusEvent
	conns   []net.Conn
}

func startFakeScheduler(t *testing.T) (*fakeScheduler, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "scheduler.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	f := &fakeScheduler{}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			f.mu.Lock()
			f.accepts++
			f.conns = append(f.conns, conn)
			f.mu.Unlock()
			go func() {
				scanner := bufio.NewScanner(conn)
				for scanner.Scan() {
					var ev StatusEvent
					json.Unmarshal(scanner.Bytes(), &ev)
					f.mu.Lock()
					f.events = append(f.events, ev)
					f.mu.Unlock()
				}
			}()
		}
	}()
	return f, path
}

func (f *fakeScheduler) wait(t *testing.T, events int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		n := len(f.events)
		f.mu.Unlock()
		if n >= events {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected %d events, got %d", events, len(f.events))
}

func TestEventsReuseOneConnection(t *testing.T) {
	f, path := startFakeScheduler(t)
	c := &IPCClient{SocketPath: path, RunID: 7, Component: "mitm_delivery", Topic: "Employee", SourceName: "DELIVERY"}
	defer c.Close()

	c.SendEvent("started", "go", 0)
	for i := 0; i < 10; i++ {
		c.SendAudit("audit")
	}
	f.wait(t, 11)

	if f.accepts != 1 {
		t.Errorf("expected 1 connection, got %d", f.accepts)
	}
	if f.events[0].Type != "status" || f.events[0].Message != "Employee: DELIVERY: go" || f.events[1].Type != "audit" {
		t.Errorf("unexpected events: %+v", f.events[:2])
	}
}

func TestReconnectAfterBrokenConnection(t *testing.T) {
	f, path := startFakeScheduler(t)
	c := &IPCClient{SocketPath: path, RunID: 7}
	defer c.Close()

	c.SendAudit("first")
	f.wait(t, 1)

	// Break the client side; the next send must re-dial.
	c.mu.Lock()
	c.conn.Close()
	c.mu.Unlock()

	c.SendAudit("second")
	f.wait(t, 2)
	if f.accepts != 2 {
		t.Errorf("expected a reconnect (2 connections), got %d", f.accepts)
	}
}

func TestNilClientIsSafe(t *testing.T) {
	var c *IPCClient
	c.SendEvent("started", "x", 0)
	c.SendAudit("x")
	c.Close()
}
