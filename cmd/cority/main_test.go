/**
 * SPDX-FileComment: Cority Delivery Batch Job Tests
 * SPDX-FileType: SOURCE
 * SPDX-FileContributor: ZHENG Robert
 * SPDX-FileCopyrightText: 2026 ZHENG Robert
 * SPDX-License-Identifier: Apache-2.0
 *
 * @file main_test.go
 * @brief Unit tests for the database connection string builder
 * @version 0.21.0
 * @date 2026-10-17
 *
 * @author ZHENG Robert (robert@hase-zheng.net)
 * @copyright Copyright (c) 2026 ZHENG Robert
 * @LICENSE Apache-2.0
 */

package main

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
