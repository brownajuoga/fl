package main

import (
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
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
	server := httptest.NewServer(router)
	defer server.Close()

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookie jar: %v", err)
	}
	client := &http.Client{
		Jar: jar,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	form := url.Values{}
	form.Set("email", defaultAdminEmail)
	form.Set("password", defaultAdminPassword)

	resp, err := client.Post(server.URL+"/login", "application/x-www-form-urlencoded", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("login request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("login status = %d, want %d", resp.StatusCode, http.StatusSeeOther)
	}
	if location := resp.Header.Get("Location"); !strings.HasPrefix(location, "/admin?flash=Welcome+") {
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
	server := httptest.NewServer(router)
	defer server.Close()

	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	registerForm := url.Values{}
	registerForm.Set("name", "Taylor Admin")
	registerForm.Set("email", "taylor@example.com")
	registerForm.Set("password", "secure123")
	registerForm.Set("confirm_password", "secure123")

	registerResp, err := client.Post(server.URL+"/register", "application/x-www-form-urlencoded", strings.NewReader(registerForm.Encode()))
	if err != nil {
		t.Fatalf("register request: %v", err)
	}
	defer registerResp.Body.Close()

	if registerResp.StatusCode != http.StatusSeeOther {
		t.Fatalf("register status = %d, want %d", registerResp.StatusCode, http.StatusSeeOther)
	}

	loginForm := url.Values{}
	loginForm.Set("email", "taylor@example.com")
	loginForm.Set("password", "secure123")

	loginResp, err := client.Post(server.URL+"/login", "application/x-www-form-urlencoded", strings.NewReader(loginForm.Encode()))
	if err != nil {
		t.Fatalf("login request: %v", err)
	}
	defer loginResp.Body.Close()

	if loginResp.StatusCode != http.StatusSeeOther {
		t.Fatalf("login status = %d, want %d", loginResp.StatusCode, http.StatusSeeOther)
	}
	if location := loginResp.Header.Get("Location"); location != "/login?flash=Your+admin+registration+is+still+waiting+for+approval" {
		t.Fatalf("login redirect target = %s, want pending approval redirect", location)
	}

	if _, err := app.db.Exec(`UPDATE players SET is_admin = 1, admin_status = 'approved' WHERE email = ?`, "taylor@example.com"); err != nil {
		t.Fatalf("approve admin: %v", err)
	}

	approvedResp, err := client.Post(server.URL+"/login", "application/x-www-form-urlencoded", strings.NewReader(loginForm.Encode()))
	if err != nil {
		t.Fatalf("approved login request: %v", err)
	}
	defer approvedResp.Body.Close()

	if approvedResp.StatusCode != http.StatusSeeOther {
		t.Fatalf("approved login status = %d, want %d", approvedResp.StatusCode, http.StatusSeeOther)
	}
	if location := approvedResp.Header.Get("Location"); !strings.HasPrefix(location, "/admin?flash=Welcome+") {
		t.Fatalf("approved login redirect target = %s, want /admin welcome redirect", location)
	}
}
