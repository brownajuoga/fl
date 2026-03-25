package main

import (
	"crypto/rand"
	"database/sql"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/crypto/bcrypt"
)

//go:embed templates/*.html templates/admin/*.html static/css/*.css static/js/*.js
var appFS embed.FS

const (
	defaultAdminEmail    = "admin@fussball.local"
	defaultAdminPassword = "admin123"
	sessionCookieName    = "fussball_session"
)

type App struct {
	db        *sql.DB
	templates *template.Template

	mu       sync.RWMutex
	sessions map[string]Session
}

type Session struct {
	PlayerID int
	Name     string
	IsAdmin  bool
	Expires  time.Time
}

type Team struct {
	ID   int
	Name string
}

type Player struct {
	ID      int
	Name    string
	Email   string
	TeamID  int
	Team    string
	IsAdmin bool
}

type TeamWithPlayers struct {
	ID          int
	Name        string
	PlayerCount int
	Players     []Player
}

type GameView struct {
	ID                  int
	GameType            string
	Status              string
	Team1Name           string
	Team2Name           string
	Team1Player1Name    string
	Team1Player2Name    string
	Team2Player1Name    string
	Team2Player2Name    string
	Team1Score          int
	Team2Score          int
	ScheduledAt         string
	PlayedAt            string
	ScheduledAtInput    string
	PlayedAtInput       string
	PlayableDescription string
}

type Standing struct {
	ID            int
	Name          string
	GamesPlayed   int
	Wins          int
	Draws         int
	Losses        int
	GoalsFor      int
	GoalsAgainst  int
	GoalDiff      int
	Points        int
	WinPercentage string
}

type StatsView struct {
	TopTeam            string
	TopTeamPoints      int
	BestAttack         string
	MostActivePlayer   string
	MostActiveGames    int
	PlayedGamesCount   int
	ScheduledGamesCount int
}

type DashboardView struct {
	Teams     []Team
	Players   []Player
	Games     []GameView
	Standings []Standing
	Rules     string
}

type TemplateData struct {
	Title         string
	PageTemplate  string
	CurrentPath   string
	Year          int
	Flash         string
	Authenticated bool
	CurrentUser   string
	DefaultAdmin  string
	DefaultPass   string

	UpcomingGames []GameView
	RecentResults []GameView
	ScheduledGames []GameView
	ResultGames   []GameView
	Standings     []Standing
	Teams         []TeamWithPlayers
	Stats         StatsView
	Dashboard     DashboardView
}

func main() {
	dbPath := envOrDefault("FUSSBALL_DB_PATH", filepath.Join("data", "fussball.db"))

	app, err := newApp(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer app.db.Close()

	server := &http.Server{
		Addr:              ":8080",
		Handler:           app.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("Fussball League running on http://localhost%s", server.Addr)
	log.Printf("Default admin login: %s / %s", defaultAdminEmail, defaultAdminPassword)
	log.Fatal(server.ListenAndServe())
}

func newApp(dbPath string) (*App, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	funcs := template.FuncMap{
		"badgeClass": func(status string) string {
			switch status {
			case "played":
				return "badge badge-played"
			case "scheduled":
				return "badge badge-scheduled"
			case "cancelled":
				return "badge badge-cancelled"
			default:
				return "badge"
			}
		},
		"gameLabel": func(gameType string) string {
			if gameType == "doubles" {
				return "Doubles"
			}
			return "Singles"
		},
		"eq": func(a, b any) bool {
			return a == b
		},
		"add1": func(v int) int {
			return v + 1
		},
	}

	tmpl, err := template.New("layout").Funcs(funcs).ParseFS(appFS, "templates/*.html", "templates/admin/*.html")
	if err != nil {
		return nil, fmt.Errorf("parse templates: %w", err)
	}

	app := &App{
		db:        db,
		templates: tmpl,
		sessions:  make(map[string]Session),
	}

	if err := app.initSchema(); err != nil {
		return nil, err
	}
	if err := app.seedData(); err != nil {
		return nil, err
	}

	return app, nil
}

func (app *App) routes() http.Handler {
	mux := http.NewServeMux()

	staticFS, err := fs.Sub(appFS, "static")
	if err != nil {
		panic(err)
	}

	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS))))

	mux.HandleFunc("/", app.homePage)
	mux.HandleFunc("/standings", app.standingsPage)
	mux.HandleFunc("/schedule", app.schedulePage)
	mux.HandleFunc("/results", app.resultsPage)
	mux.HandleFunc("/teams", app.teamsPage)
	mux.HandleFunc("/stats", app.statsPage)
	mux.HandleFunc("/login", app.loginPage)
	mux.HandleFunc("/logout", app.logout)
	mux.Handle("/admin", app.requireAdmin(http.HandlerFunc(app.adminDashboard)))
	mux.Handle("/admin/", app.requireAdmin(http.HandlerFunc(app.adminRoutes)))

	return app.logging(mux)
}

func (app *App) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s", r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
	})
}

func (app *App) homePage(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	upcoming, err := app.listGames("WHERE g.status = 'scheduled'", "ORDER BY g.scheduled_date ASC", 6)
	if err != nil {
		app.serverError(w, err)
		return
	}
	results, err := app.listGames("WHERE g.status = 'played'", "ORDER BY g.played_date DESC", 5)
	if err != nil {
		app.serverError(w, err)
		return
	}
	standings, err := app.listStandings()
	if err != nil {
		app.serverError(w, err)
		return
	}
	if len(standings) > 5 {
		standings = standings[:5]
	}

	data := app.baseData(r, "Fussball League", "index")
	data.UpcomingGames = upcoming
	data.RecentResults = results
	data.Standings = standings
	app.render(w, data)
}

func (app *App) standingsPage(w http.ResponseWriter, r *http.Request) {
	standings, err := app.listStandings()
	if err != nil {
		app.serverError(w, err)
		return
	}

	data := app.baseData(r, "Standings", "standings")
	data.Standings = standings
	app.render(w, data)
}

func (app *App) schedulePage(w http.ResponseWriter, r *http.Request) {
	games, err := app.listGames("", "ORDER BY CASE g.status WHEN 'scheduled' THEN 0 WHEN 'played' THEN 1 ELSE 2 END, g.scheduled_date ASC", 0)
	if err != nil {
		app.serverError(w, err)
		return
	}

	data := app.baseData(r, "Schedule", "games")
	data.ScheduledGames = games
	app.render(w, data)
}

func (app *App) resultsPage(w http.ResponseWriter, r *http.Request) {
	results, err := app.listGames("WHERE g.status = 'played'", "ORDER BY g.played_date DESC", 0)
	if err != nil {
		app.serverError(w, err)
		return
	}

	data := app.baseData(r, "Results", "results")
	data.ResultGames = results
	app.render(w, data)
}

func (app *App) teamsPage(w http.ResponseWriter, r *http.Request) {
	teams, err := app.listTeamsWithPlayers()
	if err != nil {
		app.serverError(w, err)
		return
	}

	data := app.baseData(r, "Teams", "teams")
	data.Teams = teams
	app.render(w, data)
}

func (app *App) statsPage(w http.ResponseWriter, r *http.Request) {
	stats, err := app.loadStats()
	if err != nil {
		app.serverError(w, err)
		return
	}

	data := app.baseData(r, "Stats", "stats")
	data.Stats = stats
	app.render(w, data)
}

func (app *App) loginPage(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		data := app.baseData(r, "Admin Login", "login")
		app.render(w, data)
	case http.MethodPost:
		if err := r.ParseForm(); err != nil {
			app.serverError(w, err)
			return
		}

		email := strings.TrimSpace(strings.ToLower(r.FormValue("email")))
		password := r.FormValue("password")
		if email == "" || password == "" {
			http.Redirect(w, r, "/login?flash=Enter+your+email+and+password", http.StatusSeeOther)
			return
		}

		var playerID int
		var name, hash string
		var isAdmin bool
		err := app.db.QueryRow(`SELECT id, name, password_hash, is_admin FROM players WHERE lower(email) = ?`, email).
			Scan(&playerID, &name, &hash, &isAdmin)
		if err != nil {
			http.Redirect(w, r, "/login?flash=Invalid+login", http.StatusSeeOther)
			return
		}
		if bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) != nil {
			http.Redirect(w, r, "/login?flash=Invalid+login", http.StatusSeeOther)
			return
		}

		if err := app.createSession(w, Session{
			PlayerID: playerID,
			Name:     name,
			IsAdmin:  isAdmin,
			Expires:  time.Now().Add(24 * time.Hour),
		}); err != nil {
			app.serverError(w, err)
			return
		}

		http.Redirect(w, r, "/admin?flash=Welcome+"+urlSafe(name), http.StatusSeeOther)
	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}

func (app *App) logout(w http.ResponseWriter, r *http.Request) {
	app.destroySession(w, r)
	http.Redirect(w, r, "/?flash=You+have+been+logged+out", http.StatusSeeOther)
}

func (app *App) adminDashboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	app.renderAdmin(w, r)
}

func (app *App) adminRoutes(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/admin/teams":
		app.createTeam(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/admin/players":
		app.createPlayer(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/admin/games":
		app.createGame(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/result"):
		app.updateGameResult(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/cancel"):
		app.cancelGame(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/admin/rules":
		app.updateRules(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (app *App) createTeam(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		app.serverError(w, err)
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		http.Redirect(w, r, "/admin?flash=Team+name+is+required", http.StatusSeeOther)
		return
	}

	if _, err := app.db.Exec(`INSERT INTO teams (name) VALUES (?)`, name); err != nil {
		http.Redirect(w, r, "/admin?flash=Could+not+create+team", http.StatusSeeOther)
		return
	}

	http.Redirect(w, r, "/admin?flash=Team+created", http.StatusSeeOther)
}

func (app *App) createPlayer(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		app.serverError(w, err)
		return
	}

	name := strings.TrimSpace(r.FormValue("name"))
	email := strings.TrimSpace(strings.ToLower(r.FormValue("email")))
	password := r.FormValue("password")
	teamID, _ := strconv.Atoi(r.FormValue("team_id"))
	isAdmin := r.FormValue("is_admin") == "on"

	if name == "" || email == "" || teamID == 0 {
		http.Redirect(w, r, "/admin?flash=Player+name,+email,+and+team+are+required", http.StatusSeeOther)
		return
	}
	if password == "" {
		password = "player123"
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		app.serverError(w, err)
		return
	}

	if _, err := app.db.Exec(
		`INSERT INTO players (name, email, team_id, is_admin, password_hash) VALUES (?, ?, ?, ?, ?)`,
		name, email, teamID, isAdmin, string(hash),
	); err != nil {
		http.Redirect(w, r, "/admin?flash=Could+not+create+player", http.StatusSeeOther)
		return
	}

	http.Redirect(w, r, "/admin?flash=Player+created", http.StatusSeeOther)
}

func (app *App) createGame(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		app.serverError(w, err)
		return
	}

	gameType := strings.TrimSpace(r.FormValue("game_type"))
	if gameType != "singles" && gameType != "doubles" {
		http.Redirect(w, r, "/admin?flash=Choose+a+valid+game+type", http.StatusSeeOther)
		return
	}

	team1ID, _ := strconv.Atoi(r.FormValue("team1_id"))
	team2ID, _ := strconv.Atoi(r.FormValue("team2_id"))
	t1p1, _ := strconv.Atoi(r.FormValue("team1_player1_id"))
	t1p2, _ := strconv.Atoi(r.FormValue("team1_player2_id"))
	t2p1, _ := strconv.Atoi(r.FormValue("team2_player1_id"))
	t2p2, _ := strconv.Atoi(r.FormValue("team2_player2_id"))
	scheduledAt, err := parseDateTimeLocal(r.FormValue("scheduled_date"))
	if err != nil {
		http.Redirect(w, r, "/admin?flash=Provide+a+valid+scheduled+date", http.StatusSeeOther)
		return
	}
	session, _ := app.currentSession(r)

	if err := validateGame(gameType, team1ID, team2ID, t1p1, t1p2, t2p1, t2p2); err != nil {
		http.Redirect(w, r, "/admin?flash="+urlSafe(err.Error()), http.StatusSeeOther)
		return
	}

	if gameType == "singles" {
		t1p2 = 0
		t2p2 = 0
	}

	if _, err := app.db.Exec(
		`INSERT INTO games (
			game_type, team1_id, team2_id, team1_player1_id, team1_player2_id,
			team2_player1_id, team2_player2_id, scheduled_date, created_by
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		gameType, team1ID, team2ID, nullableInt(t1p1), nullableInt(t1p2), nullableInt(t2p1), nullableInt(t2p2), scheduledAt.Format(time.RFC3339), session.PlayerID,
	); err != nil {
		http.Redirect(w, r, "/admin?flash=Could+not+create+game", http.StatusSeeOther)
		return
	}

	http.Redirect(w, r, "/admin?flash=Game+scheduled", http.StatusSeeOther)
}

func (app *App) updateGameResult(w http.ResponseWriter, r *http.Request) {
	gameID, err := parseGameID(r.URL.Path, "result")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		app.serverError(w, err)
		return
	}

	team1Score, err1 := strconv.Atoi(r.FormValue("team1_score"))
	team2Score, err2 := strconv.Atoi(r.FormValue("team2_score"))
	if err1 != nil || err2 != nil || team1Score < 0 || team2Score < 0 {
		http.Redirect(w, r, "/admin?flash=Enter+valid+scores", http.StatusSeeOther)
		return
	}

	if _, err := app.db.Exec(
		`UPDATE games SET team1_score = ?, team2_score = ?, status = 'played', played_date = ? WHERE id = ?`,
		team1Score, team2Score, time.Now().Format(time.RFC3339), gameID,
	); err != nil {
		http.Redirect(w, r, "/admin?flash=Could+not+save+result", http.StatusSeeOther)
		return
	}

	http.Redirect(w, r, "/admin?flash=Result+saved", http.StatusSeeOther)
}

func (app *App) cancelGame(w http.ResponseWriter, r *http.Request) {
	gameID, err := parseGameID(r.URL.Path, "cancel")
	if err != nil {
		http.NotFound(w, r)
		return
	}

	if _, err := app.db.Exec(`UPDATE games SET status = 'cancelled' WHERE id = ?`, gameID); err != nil {
		http.Redirect(w, r, "/admin?flash=Could+not+cancel+game", http.StatusSeeOther)
		return
	}

	http.Redirect(w, r, "/admin?flash=Game+cancelled", http.StatusSeeOther)
}

func (app *App) updateRules(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		app.serverError(w, err)
		return
	}
	rules := strings.TrimSpace(r.FormValue("rules"))
	if rules == "" {
		http.Redirect(w, r, "/admin?flash=Rules+cannot+be+empty", http.StatusSeeOther)
		return
	}

	if _, err := app.db.Exec(
		`INSERT INTO settings (key, value, updated_at) VALUES ('league_rules', ?, CURRENT_TIMESTAMP)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP`,
		rules,
	); err != nil {
		http.Redirect(w, r, "/admin?flash=Could+not+save+rules", http.StatusSeeOther)
		return
	}

	http.Redirect(w, r, "/admin?flash=Rules+updated", http.StatusSeeOther)
}

func (app *App) renderAdmin(w http.ResponseWriter, r *http.Request) {
	teams, err := app.listTeams()
	if err != nil {
		app.serverError(w, err)
		return
	}
	players, err := app.listPlayers()
	if err != nil {
		app.serverError(w, err)
		return
	}
	games, err := app.listGames("", "ORDER BY g.created_at DESC", 12)
	if err != nil {
		app.serverError(w, err)
		return
	}
	standings, err := app.listStandings()
	if err != nil {
		app.serverError(w, err)
		return
	}
	rules, err := app.getRules()
	if err != nil {
		app.serverError(w, err)
		return
	}

	data := app.baseData(r, "Admin Dashboard", "admin_dashboard")
	data.Dashboard = DashboardView{
		Teams:     teams,
		Players:   players,
		Games:     games,
		Standings: standings,
		Rules:     rules,
	}
	app.render(w, data)
}

func (app *App) baseData(r *http.Request, title, page string) TemplateData {
	data := TemplateData{
		Title:        title,
		PageTemplate: page,
		CurrentPath:  r.URL.Path,
		Year:         time.Now().Year(),
		Flash:        r.URL.Query().Get("flash"),
		DefaultAdmin: defaultAdminEmail,
		DefaultPass:  defaultAdminPassword,
	}

	if session, ok := app.currentSession(r); ok {
		data.Authenticated = true
		data.CurrentUser = session.Name
	}

	return data
}

func (app *App) render(w http.ResponseWriter, data TemplateData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := app.templates.ExecuteTemplate(w, "layout", data); err != nil {
		app.serverError(w, err)
	}
}

func (app *App) serverError(w http.ResponseWriter, err error) {
	log.Printf("server error: %v", err)
	http.Error(w, "Internal Server Error", http.StatusInternalServerError)
}

func (app *App) requireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		session, ok := app.currentSession(r)
		if !ok || !session.IsAdmin {
			http.Redirect(w, r, "/login?flash=Please+log+in+as+an+admin", http.StatusSeeOther)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (app *App) createSession(w http.ResponseWriter, session Session) error {
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return err
	}
	token := hex.EncodeToString(tokenBytes)

	app.mu.Lock()
	app.sessions[token] = session
	app.mu.Unlock()

	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Expires:  session.Expires,
	})
	return nil
}

func (app *App) currentSession(r *http.Request) (Session, bool) {
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil {
		return Session{}, false
	}

	app.mu.RLock()
	session, ok := app.sessions[cookie.Value]
	app.mu.RUnlock()
	if !ok || session.Expires.Before(time.Now()) {
		return Session{}, false
	}

	return session, true
}

func (app *App) destroySession(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie(sessionCookieName); err == nil {
		app.mu.Lock()
		delete(app.sessions, cookie.Value)
		app.mu.Unlock()
	}

	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Expires:  time.Unix(0, 0),
		MaxAge:   -1,
	})
}

func (app *App) initSchema() error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS teams (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT UNIQUE NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS players (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			email TEXT UNIQUE,
			team_id INTEGER,
			is_admin BOOLEAN DEFAULT 0,
			password_hash TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (team_id) REFERENCES teams(id)
		);`,
		`CREATE TABLE IF NOT EXISTS games (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			game_type TEXT NOT NULL,
			team1_id INTEGER,
			team2_id INTEGER,
			team1_player1_id INTEGER,
			team1_player2_id INTEGER,
			team2_player1_id INTEGER,
			team2_player2_id INTEGER,
			team1_score INTEGER DEFAULT 0,
			team2_score INTEGER DEFAULT 0,
			status TEXT DEFAULT 'scheduled',
			scheduled_date DATETIME,
			played_date DATETIME,
			created_by INTEGER,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (team1_id) REFERENCES teams(id),
			FOREIGN KEY (team2_id) REFERENCES teams(id),
			FOREIGN KEY (team1_player1_id) REFERENCES players(id),
			FOREIGN KEY (team1_player2_id) REFERENCES players(id),
			FOREIGN KEY (team2_player1_id) REFERENCES players(id),
			FOREIGN KEY (team2_player2_id) REFERENCES players(id)
		);`,
		`CREATE TABLE IF NOT EXISTS settings (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);`,
		`DROP VIEW IF EXISTS standings;`,
		`CREATE VIEW standings AS
		SELECT
			t.id,
			t.name,
			COUNT(CASE WHEN g.status = 'played' THEN 1 END) AS games_played,
			SUM(CASE
				WHEN g.status = 'played' AND g.team1_id = t.id AND g.team1_score > g.team2_score THEN 1
				WHEN g.status = 'played' AND g.team2_id = t.id AND g.team2_score > g.team1_score THEN 1
				ELSE 0
			END) AS wins,
			SUM(CASE
				WHEN g.status = 'played' AND g.team1_id = t.id AND g.team1_score = g.team2_score THEN 1
				WHEN g.status = 'played' AND g.team2_id = t.id AND g.team2_score = g.team1_score THEN 1
				ELSE 0
			END) AS draws,
			SUM(CASE
				WHEN g.status = 'played' AND g.team1_id = t.id AND g.team1_score < g.team2_score THEN 1
				WHEN g.status = 'played' AND g.team2_id = t.id AND g.team2_score < g.team1_score THEN 1
				ELSE 0
			END) AS losses,
			SUM(CASE
				WHEN g.status = 'played' AND g.team1_id = t.id THEN g.team1_score
				WHEN g.status = 'played' AND g.team2_id = t.id THEN g.team2_score
				ELSE 0
			END) AS goals_for,
			SUM(CASE
				WHEN g.status = 'played' AND g.team1_id = t.id THEN g.team2_score
				WHEN g.status = 'played' AND g.team2_id = t.id THEN g.team1_score
				ELSE 0
			END) AS goals_against
		FROM teams t
		LEFT JOIN games g ON (g.team1_id = t.id OR g.team2_id = t.id)
		GROUP BY t.id;`,
	}

	for _, statement := range statements {
		if _, err := app.db.Exec(statement); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}
	}

	return nil
}

func (app *App) seedData() error {
	var teamCount int
	if err := app.db.QueryRow(`SELECT COUNT(*) FROM teams`).Scan(&teamCount); err != nil {
		return fmt.Errorf("count teams: %w", err)
	}
	if teamCount > 0 {
		return nil
	}

	tx, err := app.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	teamNames := []string{"Red Lions", "Blue Rockets", "Golden Boots", "Night Owls"}
	for _, name := range teamNames {
		if _, err := tx.Exec(`INSERT INTO teams (name) VALUES (?)`, name); err != nil {
			return err
		}
	}

	rows, err := tx.Query(`SELECT id, name FROM teams ORDER BY id`)
	if err != nil {
		return err
	}
	teamIDs := make(map[string]int)
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			rows.Close()
			return err
		}
		teamIDs[name] = id
	}
	rows.Close()

	adminHash, err := bcrypt.GenerateFromPassword([]byte(defaultAdminPassword), bcrypt.DefaultCost)
	if err != nil {
		return err
	}

	playerSeed := []struct {
		Name     string
		Email    string
		TeamName string
		Admin    bool
		Password string
	}{
		{"Mila Coach", defaultAdminEmail, "Red Lions", true, defaultAdminPassword},
		{"Ari Swift", "ari@fussball.local", "Red Lions", false, "player123"},
		{"Nora Pace", "nora@fussball.local", "Blue Rockets", false, "player123"},
		{"Leo Kane", "leo@fussball.local", "Blue Rockets", false, "player123"},
		{"Sami Volt", "sami@fussball.local", "Golden Boots", false, "player123"},
		{"Ivy Cross", "ivy@fussball.local", "Golden Boots", false, "player123"},
		{"Noah Edge", "noah@fussball.local", "Night Owls", false, "player123"},
		{"Zuri Field", "zuri@fussball.local", "Night Owls", false, "player123"},
	}

	playerIDs := make(map[string]int)
	for _, p := range playerSeed {
		hash := adminHash
		if p.Password != defaultAdminPassword {
			hash, err = bcrypt.GenerateFromPassword([]byte(p.Password), bcrypt.DefaultCost)
			if err != nil {
				return err
			}
		}
		result, err := tx.Exec(
			`INSERT INTO players (name, email, team_id, is_admin, password_hash) VALUES (?, ?, ?, ?, ?)`,
			p.Name, p.Email, teamIDs[p.TeamName], p.Admin, string(hash),
		)
		if err != nil {
			return err
		}
		id, _ := result.LastInsertId()
		playerIDs[p.Name] = int(id)
	}

	now := time.Now()
	games := []struct {
		GameType  string
		Team1     string
		Team2     string
		T1P1      string
		T1P2      string
		T2P1      string
		T2P2      string
		Status    string
		Score1    int
		Score2    int
		Scheduled time.Time
		Played    time.Time
	}{
		{"singles", "Red Lions", "Blue Rockets", "Mila Coach", "", "Nora Pace", "", "played", 4, 2, now.AddDate(0, 0, -7), now.AddDate(0, 0, -7)},
		{"doubles", "Golden Boots", "Night Owls", "Sami Volt", "Ivy Cross", "Noah Edge", "Zuri Field", "played", 3, 3, now.AddDate(0, 0, -3), now.AddDate(0, 0, -3)},
		{"singles", "Blue Rockets", "Golden Boots", "Leo Kane", "", "Sami Volt", "", "scheduled", 0, 0, now.AddDate(0, 0, 2), time.Time{}},
		{"doubles", "Night Owls", "Red Lions", "Noah Edge", "Zuri Field", "Mila Coach", "Ari Swift", "scheduled", 0, 0, now.AddDate(0, 0, 5), time.Time{}},
	}

	for _, game := range games {
		var playedAt any
		if !game.Played.IsZero() {
			playedAt = game.Played.Format(time.RFC3339)
		}
		if _, err := tx.Exec(
			`INSERT INTO games (
				game_type, team1_id, team2_id, team1_player1_id, team1_player2_id,
				team2_player1_id, team2_player2_id, team1_score, team2_score, status,
				scheduled_date, played_date, created_by
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			game.GameType,
			teamIDs[game.Team1],
			teamIDs[game.Team2],
			nullableInt(playerIDs[game.T1P1]),
			nullableInt(playerIDs[game.T1P2]),
			nullableInt(playerIDs[game.T2P1]),
			nullableInt(playerIDs[game.T2P2]),
			game.Score1,
			game.Score2,
			game.Status,
			game.Scheduled.Format(time.RFC3339),
			playedAt,
			playerIDs["Mila Coach"],
		); err != nil {
			return err
		}
	}

	defaultRules := strings.Join([]string{
		"1. Win = 3 points, draw = 1 point, loss = 0 points.",
		"2. Singles matches require one player from each team.",
		"3. Doubles matches require two players from each team.",
		"4. Admins can schedule, score, and cancel matches from the dashboard.",
	}, "\n")
	if _, err := tx.Exec(`INSERT INTO settings (key, value) VALUES ('league_rules', ?)`, defaultRules); err != nil {
		return err
	}

	return tx.Commit()
}

func (app *App) listGames(whereClause, orderClause string, limit int) ([]GameView, error) {
	query := `
		SELECT
			g.id, g.game_type, g.status,
			t1.name, t2.name,
			COALESCE(p11.name, ''), COALESCE(p12.name, ''),
			COALESCE(p21.name, ''), COALESCE(p22.name, ''),
			g.team1_score, g.team2_score,
			COALESCE(g.scheduled_date, ''), COALESCE(g.played_date, '')
		FROM games g
		JOIN teams t1 ON t1.id = g.team1_id
		JOIN teams t2 ON t2.id = g.team2_id
		LEFT JOIN players p11 ON p11.id = g.team1_player1_id
		LEFT JOIN players p12 ON p12.id = g.team1_player2_id
		LEFT JOIN players p21 ON p21.id = g.team2_player1_id
		LEFT JOIN players p22 ON p22.id = g.team2_player2_id
	`
	if whereClause != "" {
		query += " " + whereClause
	}
	if orderClause != "" {
		query += " " + orderClause
	}
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := app.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var games []GameView
	for rows.Next() {
		var g GameView
		var scheduledRaw, playedRaw string
		if err := rows.Scan(
			&g.ID, &g.GameType, &g.Status,
			&g.Team1Name, &g.Team2Name,
			&g.Team1Player1Name, &g.Team1Player2Name,
			&g.Team2Player1Name, &g.Team2Player2Name,
			&g.Team1Score, &g.Team2Score,
			&scheduledRaw, &playedRaw,
		); err != nil {
			return nil, err
		}
		g.ScheduledAt = formatDateTime(scheduledRaw)
		g.PlayedAt = formatDateTime(playedRaw)
		g.ScheduledAtInput = formatDateTimeInput(scheduledRaw)
		g.PlayableDescription = playableDescription(g)
		games = append(games, g)
	}

	return games, rows.Err()
}

func (app *App) listStandings() ([]Standing, error) {
	rows, err := app.db.Query(`
		SELECT id, name, games_played, wins, draws, losses, goals_for, goals_against
		FROM standings
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var standings []Standing
	for rows.Next() {
		var s Standing
		if err := rows.Scan(&s.ID, &s.Name, &s.GamesPlayed, &s.Wins, &s.Draws, &s.Losses, &s.GoalsFor, &s.GoalsAgainst); err != nil {
			return nil, err
		}
		s.GoalDiff = s.GoalsFor - s.GoalsAgainst
		s.Points = s.Wins*3 + s.Draws
		if s.GamesPlayed > 0 {
			s.WinPercentage = fmt.Sprintf("%.0f%%", float64(s.Wins)/float64(s.GamesPlayed)*100)
		} else {
			s.WinPercentage = "0%"
		}
		standings = append(standings, s)
	}

	sort.SliceStable(standings, func(i, j int) bool {
		if standings[i].Points != standings[j].Points {
			return standings[i].Points > standings[j].Points
		}
		if standings[i].GoalDiff != standings[j].GoalDiff {
			return standings[i].GoalDiff > standings[j].GoalDiff
		}
		if standings[i].GoalsFor != standings[j].GoalsFor {
			return standings[i].GoalsFor > standings[j].GoalsFor
		}
		return standings[i].Name < standings[j].Name
	})

	return standings, nil
}

func (app *App) listTeamsWithPlayers() ([]TeamWithPlayers, error) {
	teams, err := app.listTeams()
	if err != nil {
		return nil, err
	}
	players, err := app.listPlayers()
	if err != nil {
		return nil, err
	}

	grouped := make(map[int][]Player)
	for _, player := range players {
		grouped[player.TeamID] = append(grouped[player.TeamID], player)
	}

	teamViews := make([]TeamWithPlayers, 0, len(teams))
	for _, team := range teams {
		teamPlayers := grouped[team.ID]
		teamViews = append(teamViews, TeamWithPlayers{
			ID:          team.ID,
			Name:        team.Name,
			PlayerCount: len(teamPlayers),
			Players:     teamPlayers,
		})
	}

	return teamViews, nil
}

func (app *App) listTeams() ([]Team, error) {
	rows, err := app.db.Query(`SELECT id, name FROM teams ORDER BY name ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var teams []Team
	for rows.Next() {
		var team Team
		if err := rows.Scan(&team.ID, &team.Name); err != nil {
			return nil, err
		}
		teams = append(teams, team)
	}

	return teams, rows.Err()
}

func (app *App) listPlayers() ([]Player, error) {
	rows, err := app.db.Query(`
		SELECT p.id, p.name, COALESCE(p.email, ''), COALESCE(p.team_id, 0), COALESCE(t.name, ''), p.is_admin
		FROM players p
		LEFT JOIN teams t ON t.id = p.team_id
		ORDER BY p.name ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var players []Player
	for rows.Next() {
		var p Player
		if err := rows.Scan(&p.ID, &p.Name, &p.Email, &p.TeamID, &p.Team, &p.IsAdmin); err != nil {
			return nil, err
		}
		players = append(players, p)
	}

	return players, rows.Err()
}

func (app *App) loadStats() (StatsView, error) {
	var stats StatsView

	standings, err := app.listStandings()
	if err != nil {
		return stats, err
	}
	if len(standings) > 0 {
		stats.TopTeam = standings[0].Name
		stats.TopTeamPoints = standings[0].Points
		bestAttack := standings[0]
		for _, standing := range standings[1:] {
			if standing.GoalsFor > bestAttack.GoalsFor {
				bestAttack = standing
			}
		}
		stats.BestAttack = bestAttack.Name
	}

	if err := app.db.QueryRow(`SELECT COUNT(*) FROM games WHERE status = 'played'`).Scan(&stats.PlayedGamesCount); err != nil {
		return stats, err
	}
	if err := app.db.QueryRow(`SELECT COUNT(*) FROM games WHERE status = 'scheduled'`).Scan(&stats.ScheduledGamesCount); err != nil {
		return stats, err
	}

	var playerName string
	var gameCount int
	err = app.db.QueryRow(`
		SELECT p.name, COUNT(*) AS appearances
		FROM players p
		JOIN games g
			ON p.id = g.team1_player1_id OR p.id = g.team1_player2_id OR p.id = g.team2_player1_id OR p.id = g.team2_player2_id
		GROUP BY p.id
		ORDER BY appearances DESC, p.name ASC
		LIMIT 1
	`).Scan(&playerName, &gameCount)
	if err == nil {
		stats.MostActivePlayer = playerName
		stats.MostActiveGames = gameCount
	}
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}

	return stats, err
}

func (app *App) getRules() (string, error) {
	var rules string
	err := app.db.QueryRow(`SELECT value FROM settings WHERE key = 'league_rules'`).Scan(&rules)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return rules, err
}

func validateGame(gameType string, team1ID, team2ID, t1p1, t1p2, t2p1, t2p2 int) error {
	if team1ID == 0 || team2ID == 0 {
		return errors.New("both teams are required")
	}
	if team1ID == team2ID {
		return errors.New("teams must be different")
	}
	if t1p1 == 0 || t2p1 == 0 {
		return errors.New("select a primary player for both teams")
	}
	if gameType == "doubles" {
		if t1p2 == 0 || t2p2 == 0 {
			return errors.New("doubles games need two players per team")
		}
		if t1p1 == t1p2 || t2p1 == t2p2 {
			return errors.New("choose two different players per team")
		}
	}
	return nil
}

func parseGameID(path, action string) (int, error) {
	trimmed := strings.TrimPrefix(path, "/admin/games/")
	trimmed = strings.TrimSuffix(trimmed, "/"+action)
	return strconv.Atoi(trimmed)
}

func parseDateTimeLocal(value string) (time.Time, error) {
	return time.Parse("2006-01-02T15:04", value)
}

func formatDateTime(raw string) string {
	if raw == "" {
		return "TBD"
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02 15:04"} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.Format("02 Jan 2006, 15:04")
		}
	}
	return raw
}

func formatDateTimeInput(raw string) string {
	if raw == "" {
		return ""
	}
	if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
		return parsed.Format("2006-01-02T15:04")
	}
	return ""
}

func playableDescription(game GameView) string {
	team1Players := []string{game.Team1Player1Name}
	if game.Team1Player2Name != "" {
		team1Players = append(team1Players, game.Team1Player2Name)
	}
	team2Players := []string{game.Team2Player1Name}
	if game.Team2Player2Name != "" {
		team2Players = append(team2Players, game.Team2Player2Name)
	}
	return strings.Join(team1Players, " & ") + " vs " + strings.Join(team2Players, " & ")
}

func nullableInt(v int) any {
	if v == 0 {
		return nil
	}
	return v
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func urlSafe(value string) string {
	replacer := strings.NewReplacer(" ", "+", "&", "and")
	return replacer.Replace(value)
}
