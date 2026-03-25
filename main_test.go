package main

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestPublicPagesAndAdminRedirect(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	app, err := newApp(dbPath)
	if err != nil {
		t.Fatalf("newApp: %v", err)
	}
	defer app.db.Close()

	router := app.routes()

	homeReq := httptest.NewRequest(http.MethodGet, "/", nil)
	homeRec := httptest.NewRecorder()
	router.ServeHTTP(homeRec, homeReq)
	if homeRec.Code != http.StatusOK {
		t.Fatalf("home status = %d, want %d", homeRec.Code, http.StatusOK)
	}

	adminReq := httptest.NewRequest(http.MethodGet, "/admin", nil)
	adminRec := httptest.NewRecorder()
	router.ServeHTTP(adminRec, adminReq)
	if adminRec.Code != http.StatusSeeOther {
		t.Fatalf("admin status = %d, want %d", adminRec.Code, http.StatusSeeOther)
	}
	if location := adminRec.Header().Get("Location"); location != "/login?flash=Please+log+in+as+an+admin" {
		t.Fatalf("admin redirect target = %s, want login redirect", location)
	}
}
