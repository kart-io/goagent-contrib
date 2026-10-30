// Package practical 提供实用工具的测试
// 本文件测试 API Caller、File Operations、Database Query 和 Web Scraper 等实用工具的功能
package database

import (
	"strings"
	"testing"
)

// API Caller Tests
// API 调用工具测试

// TestNewDatabaseQueryTool 测试创建数据库查询工具
func TestNewDatabaseQueryTool(t *testing.T) {
	tool := NewDatabaseQueryTool()

	if tool == nil {
		t.Fatal("Expected non-nil tool")
	}

	if tool.Name() != "database_query" {
		t.Errorf("Expected name 'database_query', got: %s", tool.Name())
	}

	if tool.Description() == "" {
		t.Error("Expected non-empty description")
	}

	if tool.ArgsSchema() == "" {
		t.Error("Expected non-empty args schema")
	}

	if tool.maxRows != 1000 {
		t.Errorf("Expected max rows 1000, got: %d", tool.maxRows)
	}
}

// TestDatabaseQueryTool_AddConnection 测试添加数据库连接
func TestDatabaseQueryTool_AddConnection(t *testing.T) {
	tool := NewDatabaseQueryTool()

	// Since we can't test real database connections in unit tests,
	// we just verify the tool structure
	if tool.connections == nil {
		t.Error("Expected connections map to be initialized")
	}

	// Verify default values
	if tool.maxRows != 1000 {
		t.Errorf("Expected maxRows 1000, got: %d", tool.maxRows)
	}
}

// TestDatabaseQueryTool_Name 测试获取工具名称
func TestDatabaseQueryTool_Name(t *testing.T) {
	tool := NewDatabaseQueryTool()
	if tool.Name() != "database_query" {
		t.Errorf("Expected name 'database_query', got: %s", tool.Name())
	}
}

// TestDatabaseQueryTool_Description 测试获取工具描述
func TestDatabaseQueryTool_Description(t *testing.T) {
	tool := NewDatabaseQueryTool()
	desc := tool.Description()
	if !strings.Contains(desc, "SQL") {
		t.Errorf("Expected description to mention SQL, got: %s", desc)
	}
}

// Web Scraper Tool Tests
// 网页抓取工具测试
