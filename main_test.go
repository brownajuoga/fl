package main

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
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

func TestAdminLoginWithStoredEmail(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	app, err := newApp(dbPath)
	if err != nil {
		t.Fatalf("newApp: %v", err)
	}
	defer app.db.Close()

	router := app.routes()
	registerBody := strings.NewReader("name=First+Admin&email=firstadmin%40example.com&password=secure123&confirm_password=secure123")
	registerReq := httptest.NewRequest(http.MethodPost, "/register", registerBody)
	registerReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	registerRec := httptest.NewRecorder()
	router.ServeHTTP(registerRec, registerReq)
	if registerRec.Code != http.StatusSeeOther {
		t.Fatalf("register status = %d, want %d", registerRec.Code, http.StatusSeeOther)
	}

	loginBody := strings.NewReader("email=firstadmin%40example.com&password=secure123")
	loginReq := httptest.NewRequest(http.MethodPost, "/login", loginBody)
	loginReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	loginRec := httptest.NewRecorder()
	router.ServeHTTP(loginRec, loginReq)

	if loginRec.Code != http.StatusSeeOther {
		t.Fatalf("login status = %d, want %d", loginRec.Code, http.StatusSeeOther)
	}
	if location := loginRec.Header().Get("Location"); !strings.HasPrefix(location, "/admin?flash=Welcome+") {
		t.Fatalf("login redirect target = %s, want /admin welcome redirect", location)
	}
}

func TestAdminRegistrationRequiresApprovalBeforeLogin(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	app, err := newApp(dbPath)
	if err != nil {
		t.Fatalf("newApp: %v", err)
	}
	defer app.db.Close()

	router := app.routes()
	firstAdminReq := httptest.NewRequest(http.MethodPost, "/register", strings.NewReader("name=Bootstrap+Admin&email=bootstrap%40example.com&password=secure123&confirm_password=secure123"))
	firstAdminReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	firstAdminRec := httptest.NewRecorder()
	router.ServeHTTP(firstAdminRec, firstAdminReq)

	registerReq := httptest.NewRequest(http.MethodPost, "/register", strings.NewReader("name=Taylor+Admin&email=taylor%40example.com&password=secure123&confirm_password=secure123"))
	registerReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	registerRec := httptest.NewRecorder()
	router.ServeHTTP(registerRec, registerReq)

	if registerRec.Code != http.StatusSeeOther {
		t.Fatalf("register status = %d, want %d", registerRec.Code, http.StatusSeeOther)
	}

	loginReq := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader("email=taylor%40example.com&password=secure123"))
	loginReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	loginRec := httptest.NewRecorder()
	router.ServeHTTP(loginRec, loginReq)

	if loginRec.Code != http.StatusSeeOther {
		t.Fatalf("login status = %d, want %d", loginRec.Code, http.StatusSeeOther)
	}
	if location := loginRec.Header().Get("Location"); location != "/login?flash=Your+admin+registration+is+still+waiting+for+approval" {
		t.Fatalf("login redirect target = %s, want pending approval redirect", location)
	}

	if _, err := app.db.Exec(`UPDATE players SET is_admin = 1, admin_status = 'approved' WHERE email = ?`, "taylor@example.com"); err != nil {
		t.Fatalf("approve admin: %v", err)
	}

	approvedReq := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader("email=taylor%40example.com&password=secure123"))
	approvedReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	approvedRec := httptest.NewRecorder()
	router.ServeHTTP(approvedRec, approvedReq)

	if approvedRec.Code != http.StatusSeeOther {
		t.Fatalf("approved login status = %d, want %d", approvedRec.Code, http.StatusSeeOther)
	}
	if location := approvedRec.Header().Get("Location"); !strings.HasPrefix(location, "/admin?flash=Welcome+") {
		t.Fatalf("approved login redirect target = %s, want /admin welcome redirect", location)
	}
}
