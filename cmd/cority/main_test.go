/**
 * SPDX-FileComment: Cority Delivery Batch Job Tests
 * SPDX-FileType: SOURCE
 * SPDX-FileContributor: ZHENG Robert
 * SPDX-FileCopyrightText: 2026 ZHENG Robert
 * SPDX-License-Identifier: Apache-2.0
 *
 * @file main_test.go
 * @brief Unit tests for the database connection string builder and IPC exit reporting
 * @version 0.22.1
 * @date 2026-11-17
 *
 * @author ZHENG Robert (robert@hase-zheng.net)
 * @copyright Copyright (c) 2026 ZHENG Robert
 * @LICENSE Apache-2.0
 */

package main

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"mitm_cority/internal/ipc"
)

type testScheduler struct {
	mu     sync.Mutex
	events []ipc.StatusEvent
}

func startTestScheduler(t *testing.T) (*testScheduler, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "scheduler.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	s := &testScheduler{}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				scanner := bufio.NewScanner(c)
				for scanner.Scan() {
					var raw map[string]interface{}
					if err := json.Unmarshal(scanner.Bytes(), &raw); err == nil && raw["type"] == "get_credentials" {
						_, _ = c.Write([]byte(`{"master_key":"","db_config_json":""}` + "\n"))
						continue
					}
					var ev ipc.StatusEvent
					if err := json.Unmarshal(scanner.Bytes(), &ev); err == nil {
						s.mu.Lock()
						s.events = append(s.events, ev)
						s.mu.Unlock()
					}
				}
			}(conn)
		}
	}()
	return s, path
}

func (s *testScheduler) waitCount(t *testing.T, want int) []ipc.StatusEvent {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		n := len(s.events)
		if n >= want {
			out := append([]ipc.StatusEvent(nil), s.events...)
			s.mu.Unlock()
			return out
		}
		s.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t.Fatalf("expected %d IPC events, got %d: %+v", want, len(s.events), s.events)
	return nil
}

func TestBuildConnStringEscapesCredentials(t *testing.T) {
	user, pass := "mitm@user", "p@ss:w/rd?#%x"
	conn := buildConnString(user, pass, "192.168.7.31", "5432", "mitm", "disable")

	cfg, err := pgxpool.ParseConfig(conn)
	if err != nil {
		t.Fatalf("connection string not parseable: %v", err)
	}
	cc := cfg.ConnConfig
	if cc.User != user || cc.Password != pass {
		t.Errorf("credentials not preserved: user=%q pass=%q", cc.User, cc.Password)
	}
	if cc.Host != "192.168.7.31" || cc.Port != 5432 || cc.Database != "mitm" {
		t.Errorf("unexpected host/port/db: %s:%d/%s", cc.Host, cc.Port, cc.Database)
	}
}

func TestStartSystemLogMessageIncludesVersion(t *testing.T) {
	orig := version
	t.Cleanup(func() { version = orig })

	version = "0.22.1"
	got := startSystemLogMessage("Delivery Employee")
	want := "Starting job Delivery Employee (v0.22.1)"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	version = "v0.22.1-3-gabcdef"
	got = startSystemLogMessage(resolveJobName(nil, nil, 0, "Employee"))
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFailfSendsEventAndAudit(t *testing.T) {
	sched, sock := startTestScheduler(t)
	client := &ipc.IPCClient{
		SocketPath: sock,
		RunID:      42,
		Component:  "mitm_delivery",
		Topic:      "Employee",
		SourceName: "DELIVERY",
	}
	defer client.Close()

	code := failf(client, nil, "Delivery Employee", "target config broken: %s", "bad key")
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}

	events := sched.waitCount(t, 3)
	if events[0].Type != "status" || events[0].Status != "failed" || !strings.Contains(events[0].Message, "target config broken: bad key") {
		t.Errorf("unexpected status event: %+v", events[0])
	}
	if events[1].Type != "audit" || !strings.Contains(events[1].Message, "ERROR: target config broken: bad key") {
		t.Errorf("unexpected error audit event: %+v", events[1])
	}
	if events[2].Type != "audit" || !strings.HasSuffix(events[2].Message, "failed") {
		t.Errorf("unexpected final audit event: %+v", events[2])
	}
}

func TestExitGracefullySendsEventAndAudit(t *testing.T) {
	sched, sock := startTestScheduler(t)
	client := &ipc.IPCClient{
		SocketPath: sock,
		RunID:      42,
		Component:  "mitm_delivery",
		Topic:      "Employee",
		SourceName: "DELIVERY",
	}
	defer client.Close()

	code := exitGracefully(client, nil, "Delivery Employee", "Another delivery instance is already running.")
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d", code)
	}

	events := sched.waitCount(t, 3)
	if events[0].Type != "audit" || !strings.Contains(events[0].Message, "Another delivery instance is already running.") {
		t.Errorf("unexpected reason audit event: %+v", events[0])
	}
	if events[1].Type != "status" || events[1].Status != "exited" {
		t.Errorf("unexpected exited status event: %+v", events[1])
	}
	if events[2].Type != "audit" || !strings.HasSuffix(events[2].Message, "exited") {
		t.Errorf("unexpected final audit event: %+v", events[2])
	}
}

func TestFinishRunStatusesSendEventAndAudit(t *testing.T) {
	cases := []struct {
		status     string
		exitCode   int
		wantErrPfx bool
	}{
		{"finished", 0, false},
		{"failed", 2, true},
		{"timeout", 0, false},
		{"cancelled", 0, false},
	}

	for _, tc := range cases {
		t.Run(tc.status, func(t *testing.T) {
			sched, sock := startTestScheduler(t)
			client := &ipc.IPCClient{
				SocketPath: sock,
				RunID:      99,
				Component:  "mitm_delivery",
				Topic:      "Org",
				SourceName: "DELIVERY",
			}
			defer client.Close()

			code := finishRun(client, nil, "Delivery Org", tc.status, "Topic Org: 1 delivered", tc.exitCode)
			if code != tc.exitCode {
				t.Fatalf("expected exit code %d, got %d", tc.exitCode, code)
			}

			events := sched.waitCount(t, 3)
			hasErrPfx := strings.Contains(events[0].Message, "ERROR: Topic Org: 1 delivered")
			if events[0].Type != "audit" || hasErrPfx != tc.wantErrPfx {
				t.Errorf("unexpected summary audit event: %+v", events[0])
			}
			if events[1].Type != "status" || events[1].Status != tc.status || events[1].Progress != 100 {
				t.Errorf("unexpected terminal status event: %+v", events[1])
			}
			if events[2].Type != "audit" || !strings.HasSuffix(events[2].Message, tc.status) {
				t.Errorf("unexpected final audit event: %+v", events[2])
			}
		})
	}
}

func TestRunEarlyExitEmitsStartAndFailOverIPC(t *testing.T) {
	sched, sock := startTestScheduler(t)
	t.Setenv("RUN_ID", "77")
	t.Setenv("SCHEDULER_SOCKET_PATH", sock)
	t.Setenv("MITM_DB_CONFIG_JSON", "{invalid-json")

	origArgs := os.Args
	t.Cleanup(func() { os.Args = origArgs })
	os.Args = []string{"mitm-deliver-cority", `{"topic":"Employee","source_name":"CORITY_EMP"}`}

	code := run()
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}

	// Expected: started (status), started (audit), failed (status), ERROR (audit), failed (audit)
	events := sched.waitCount(t, 5)
	if events[0].Type != "status" || events[0].Status != "started" {
		t.Errorf("expected started status event, got %+v", events[0])
	}
	if events[1].Type != "audit" || !strings.HasSuffix(events[1].Message, "started") {
		t.Errorf("expected started audit event, got %+v", events[1])
	}
	if events[2].Type != "status" || events[2].Status != "failed" || !strings.HasPrefix(events[2].Message, "Employee: CORITY_EMP: ") {
		t.Errorf("expected prefixed failed status event, got %+v", events[2])
	}
	if events[3].Type != "audit" || !strings.Contains(events[3].Message, "ERROR: Failed to parse MitM JSON configuration") {
		t.Errorf("expected ERROR audit event, got %+v", events[3])
	}
	if events[4].Type != "audit" || !strings.HasSuffix(events[4].Message, "failed") {
		t.Errorf("expected final failed audit event, got %+v", events[4])
	}
}
