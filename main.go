package main

import (
	"crypto/rand"
	"database/sql"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/mail"
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

const sessionCookieName = "fussball_session"

var kenyaLocation = time.FixedZone("Africa/Nairobi", 3*60*60)

type App struct {
	db        *sql.DB
	dbPath    string
	templates *template.Template

	mu              sync.RWMutex
	sessions        map[string]Session
	refereeSessions map[string]RefereeSession
}

type Session struct {
	PlayerID int
	Name     string
	IsAdmin  bool
	Expires  time.Time
}

type RefereeSession struct {
	GameID  int
	Name    string
	Expires time.Time
}

type Team struct {
	ID   int
	Name string
}

type Player struct {
	ID              int
	Name            string
	Email           string
	TeamID          int
	Team            string
	IsAdmin         bool
	AdminStatus     string
	AdminRank       int
	CanDelete       bool
	CanApprove      bool
	ProtectionLabel string
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
	LiveMinute          int
	LivePaused          bool
	Team1ID             int
	Team2ID             int
	Team1Name           string
	Team2Name           string
	Team1Player1ID      int
	Team1Player1Name    string
	Team1Player2ID      int
	Team1Player2Name    string
	Team2Player1ID      int
	Team2Player1Name    string
	Team2Player2ID      int
	Team2Player2Name    string
	Team1Score          int
	Team2Score          int
	RefereeID           int
	RefereeName         string
	RefereeCode         string
	RefereeAuthorized   bool
	RefereeSessionName  string
	Team1StrikerID      int
	Team1StrikerName    string
	Team1DefenderID     int
	Team1DefenderName   string
	Team2StrikerID      int
	Team2StrikerName    string
	Team2DefenderID     int
	Team2DefenderName   string
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
	ShowWinRate   bool
}

type StatsView struct {
	TopTeam             string
	TopTeamPoints       int
	BestAttack          string
	BestDefense         string
	TopScorer           string
	TopScorerGoals      int
	TopScoringGame      string
	TopScoringGoals     int
	MostActivePlayer    string
	MostActiveGames     int
	PlayedGamesCount    int
	ScheduledGamesCount int
	TopScorers          []PlayerStat
	TopDefenders        []PlayerStat
}

type PlayerStat struct {
	Name    string
	Team    string
	Value   int
	Minutes int
}

type AdminLog struct {
	ID        int
	AdminName string
	Action    string
	Target    string
	Details   string
	CreatedAt string
}

type DashboardView struct {
	Teams         []Team
	Players       []Player
	Admins        []Player
	PendingAdmins []Player
	Games         []GameView
	Standings     []Standing
	Rules         string
	Logs          []AdminLog
	Backups       []BackupEntry
	StatsLimit    int
	DefenderLimit int
	StatsWidgets  []string
}

type BackupEntry struct {
	Name        string
	URL         string
	CreatedAt   string
	SizeLabel   string
}

type TemplateData struct {
	Title         string
	PageTemplate  string
	CurrentPath   string
	Year          int
	Flash         string
	Authenticated bool
	CurrentUser   string
	SearchQuery   string
	ShowTeamSearch bool
	ShowGameSearch bool
	ShowLogSearch  bool
	StatsLimit     int
	DefenderLimit  int
	StatsWidgets   []string
	LiveGames      []GameView

	UpcomingGames  []GameView
	RecentResults  []GameView
	ScheduledGames []GameView
	ResultGames    []GameView
	Standings      []Standing
	Teams          []TeamWithPlayers
	Stats          StatsView
	Dashboard      DashboardView
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
	log.Printf("Admins can register at http://localhost%s/register", server.Addr)
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
			case "live":
				return "badge badge-live"
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
		"join": func(items []string, sep string) string {
			return strings.Join(items, sep)
		},
	}

	tmpl, err := template.New("layout").Funcs(funcs).ParseFS(appFS, "templates/*.html", "templates/admin/*.html")
	if err != nil {
		return nil, fmt.Errorf("parse templates: %w", err)
	}

	app := &App{
		db:              db,
		dbPath:          dbPath,
		templates:       tmpl,
		sessions:        make(map[string]Session),
		refereeSessions: make(map[string]RefereeSession),
	}

	if err := app.initSchema(); err != nil {
		return nil, err
	}
	if err := app.seedData(); err != nil {
		return nil, err
	}
	if err := app.cleanupOrphanedAdminReferences(); err != nil {
		return nil, err
	}
	if err := app.cleanupLegacyDefaultAdmin(); err != nil {
		return nil, err
	}
	if err := app.bootstrapRoleEvents(); err != nil {
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
	mux.HandleFunc("/register", app.registerPage)
	mux.HandleFunc("/logout", app.logout)
	mux.HandleFunc("/referee/access", app.refereeAccess)
	mux.HandleFunc("/referee/games/", app.refereeRoutes)
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

	upcoming, err := app.listGames("WHERE g.status IN ('scheduled', 'live')", "ORDER BY CASE g.status WHEN 'live' THEN 0 ELSE 1 END, g.scheduled_date ASC", 6)
	if err != nil {
		app.serverError(w, err)
		return
	}
	upcoming, err = app.decorateGamesForRequest(r, upcoming)
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
	data.LiveGames = filterGamesByStatus(upcoming, "live")
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

	liveGames, err := app.listGames("WHERE g.status = 'live'", "ORDER BY g.scheduled_date ASC", 0)
	if err != nil {
		app.serverError(w, err)
		return
	}
	liveGames, err = app.decorateGamesForRequest(r, liveGames)
	if err != nil {
		app.serverError(w, err)
		return
	}

	data := app.baseData(r, "Standings", "standings")
	data.Standings = standings
	data.LiveGames = liveGames
	app.render(w, data)
}

func (app *App) schedulePage(w http.ResponseWriter, r *http.Request) {
	games, err := app.listGames("", "ORDER BY CASE g.status WHEN 'live' THEN 0 WHEN 'scheduled' THEN 1 WHEN 'played' THEN 2 ELSE 3 END, g.scheduled_date ASC", 0)
	if err != nil {
		app.serverError(w, err)
		return
	}
	games, err = app.decorateGamesForRequest(r, games)
	if err != nil {
		app.serverError(w, err)
		return
	}
	showSearch := len(games) >= 6
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query != "" {
		games = filterGames(games, query)
	}

	data := app.baseData(r, "Schedule", "games")
	data.SearchQuery = query
	data.ShowGameSearch = showSearch
	data.ScheduledGames = games
	app.render(w, data)
}

func (app *App) resultsPage(w http.ResponseWriter, r *http.Request) {
	results, err := app.listGames("WHERE g.status = 'played'", "ORDER BY g.played_date DESC", 0)
	if err != nil {
		app.serverError(w, err)
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	showSearch := len(results) >= 6
	if query != "" {
		results = filterGames(results, query)
	}

	data := app.baseData(r, "Results", "results")
	data.SearchQuery = query
	data.ShowGameSearch = showSearch
	data.ResultGames = results
	app.render(w, data)
}

func (app *App) teamsPage(w http.ResponseWriter, r *http.Request) {
	teams, err := app.listTeamsWithPlayers()
	if err != nil {
		app.serverError(w, err)
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	showSearch := len(teams) >= 6
	if query != "" {
		teams = filterTeams(teams, query)
	}

	data := app.baseData(r, "Teams", "teams")
	data.SearchQuery = query
	data.ShowTeamSearch = showSearch
	data.Teams = teams
	app.render(w, data)
}

func (app *App) statsPage(w http.ResponseWriter, r *http.Request) {
	stats, err := app.loadStats()
	if err != nil {
		app.serverError(w, err)
		return
	}

	widgets, err := app.getStatsWidgets()
	if err != nil {
		app.serverError(w, err)
		return
	}
	data := app.baseData(r, "Stats", "stats")
	data.Stats = stats
	data.StatsLimit = len(stats.TopScorers)
	data.DefenderLimit = len(stats.TopDefenders)
	data.StatsWidgets = widgets
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
		var name, hash, adminStatus string
		var isAdmin bool
		err := app.db.QueryRow(`SELECT id, name, password_hash, is_admin, COALESCE(admin_status, 'none') FROM players WHERE lower(email) = ?`, email).
			Scan(&playerID, &name, &hash, &isAdmin, &adminStatus)
		if err != nil {
			http.Redirect(w, r, "/login?flash=No+admin+account+was+found+for+that+email.+Register+first", http.StatusSeeOther)
			return
		}
		if bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) != nil {
			http.Redirect(w, r, "/login?flash=Incorrect+password+for+that+email", http.StatusSeeOther)
			return
		}
		if adminStatus == "pending" {
			http.Redirect(w, r, "/login?flash=Your+admin+registration+is+still+waiting+for+approval", http.StatusSeeOther)
			return
		}
		if !isAdmin {
			http.Redirect(w, r, "/login?flash=That+email+exists,+but+it+does+not+have+admin+access", http.StatusSeeOther)
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
		app.logAdminAction(playerID, "logged in", "session", 0, fmt.Sprintf("Admin %q signed in with email %s", name, email))

		http.Redirect(w, r, "/admin?flash=Welcome+"+urlSafe(name), http.StatusSeeOther)
	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}

func (app *App) registerPage(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		data := app.baseData(r, "Admin Registration", "register")
		app.render(w, data)
	case http.MethodPost:
		if err := r.ParseForm(); err != nil {
			app.serverError(w, err)
			return
		}

		name := strings.TrimSpace(r.FormValue("name"))
		email := strings.TrimSpace(strings.ToLower(r.FormValue("email")))
		password := r.FormValue("password")
		confirmPassword := r.FormValue("confirm_password")
		if name == "" || email == "" || password == "" || confirmPassword == "" {
			http.Redirect(w, r, "/register?flash=Fill+in+all+registration+fields", http.StatusSeeOther)
			return
		}
		if _, err := mail.ParseAddress(email); err != nil {
			http.Redirect(w, r, "/register?flash=Enter+a+valid+email+address", http.StatusSeeOther)
			return
		}
		if password != confirmPassword {
			http.Redirect(w, r, "/register?flash=Passwords+do+not+match", http.StatusSeeOther)
			return
		}
		if len(password) < 6 {
			http.Redirect(w, r, "/register?flash=Password+must+be+at+least+6+characters", http.StatusSeeOther)
			return
		}

		hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if err != nil {
			app.serverError(w, err)
			return
		}

		hasApprovedAdmin, err := app.hasApprovedAdmin()
		if err != nil {
			app.serverError(w, err)
			return
		}

		isAdmin := 0
		adminStatus := "pending"
		flash := "/register?flash=Registration+received.+Wait+for+an+existing+admin+to+approve+your+account"
		if !hasApprovedAdmin {
			isAdmin = 1
			adminStatus = "approved"
			flash = "/login?flash=Your+account+is+the+first+admin.+Please+log+in"
		}

		result, err := app.db.Exec(
			`INSERT INTO players (name, email, is_admin, admin_status, password_hash) VALUES (?, ?, ?, ?, ?)`,
			name, email, isAdmin, adminStatus, string(hash),
		)
		if err != nil {
			http.Redirect(w, r, "/register?flash=That+email+is+already+registered", http.StatusSeeOther)
			return
		}

		playerID, _ := result.LastInsertId()
		action := "requested admin approval"
		details := fmt.Sprintf("Registered pending admin %q with email %s", name, email)
		if isAdmin == 1 {
			action = "bootstrapped first admin"
			details = fmt.Sprintf("Registered initial admin %q with email %s", name, email)
		}
		app.logAdminAction(int(playerID), action, "player", int(playerID), details)
		http.Redirect(w, r, flash, http.StatusSeeOther)
	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}

func (app *App) logout(w http.ResponseWriter, r *http.Request) {
	app.logAdminActionFromRequest(r, "logged out", "session", 0, "Admin signed out")
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
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/delete") && strings.Contains(r.URL.Path, "/admin/teams/"):
		app.deleteTeam(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/update") && strings.Contains(r.URL.Path, "/admin/teams/"):
		app.updateTeam(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/admin/players":
		app.createPlayer(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/delete") && strings.Contains(r.URL.Path, "/admin/players/"):
		app.deletePlayer(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/update") && strings.Contains(r.URL.Path, "/admin/players/"):
		app.updatePlayer(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/approve-admin") && strings.Contains(r.URL.Path, "/admin/players/"):
		app.approveAdmin(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/admin/games":
		app.createGame(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/referee-code"):
		app.generateRefereeCode(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/lineup"):
		app.updateGameLineupAsAdmin(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/goal"):
		app.recordGoal(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/result"):
		app.updateGameResult(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/cancel"):
		app.cancelGame(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/delete") && strings.Contains(r.URL.Path, "/admin/games/"):
		app.deleteGame(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/admin/rules":
		app.updateRules(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/admin/stats-config":
		app.updateStatsConfig(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/admin/backups/create":
		app.createBackup(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/admin/backups/"):
		app.downloadBackup(w, r)
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
		app.redirectWithReturn(w, r, "/admin", "Team+name+is+required")
		return
	}

	result, err := app.db.Exec(`INSERT INTO teams (name) VALUES (?)`, name)
	if err != nil {
		app.redirectWithReturn(w, r, "/admin", "Could+not+create+team")
		return
	}
	teamID, _ := result.LastInsertId()
	app.logAdminActionFromRequest(r, "created team", "team", int(teamID), fmt.Sprintf("Created team %q", name))

	app.redirectWithReturn(w, r, "/admin", "Team+created")
}

func (app *App) updateTeam(w http.ResponseWriter, r *http.Request) {
	teamID, err := parseEntityID(r.URL.Path, "/admin/teams/", "update")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		app.serverError(w, err)
		return
	}

	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		app.redirectWithReturn(w, r, "/admin", "Team+name+is+required")
		return
	}

	if _, err := app.db.Exec(`UPDATE teams SET name = ? WHERE id = ?`, name, teamID); err != nil {
		app.redirectWithReturn(w, r, "/admin", "Could+not+update+team")
		return
	}
	app.logAdminActionFromRequest(r, "updated team", "team", teamID, fmt.Sprintf("Updated team #%d to %q", teamID, name))
	app.redirectWithReturn(w, r, "/admin", "Team+updated")
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

	if name == "" || teamID == 0 {
		app.redirectWithReturn(w, r, "/admin", "Player+name+and+team+are+required")
		return
	}
	if email != "" {
		if _, err := mail.ParseAddress(email); err != nil {
			app.redirectWithReturn(w, r, "/admin", "Enter+a+valid+email+address")
			return
		}
	}
	if isAdmin && email == "" {
		app.redirectWithReturn(w, r, "/admin", "Admin+accounts+need+an+email+for+login")
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

	adminStatus := "none"
	if isAdmin {
		adminStatus = "approved"
	}

	var emailValue any
	if email != "" {
		emailValue = email
	}

	result, err := app.db.Exec(
		`INSERT INTO players (name, email, team_id, is_admin, admin_status, password_hash) VALUES (?, ?, ?, ?, ?, ?)`,
		name, emailValue, teamID, isAdmin, adminStatus, string(hash),
	)
	if err != nil {
		app.redirectWithReturn(w, r, "/admin", "Could+not+create+player")
		return
	}
	playerID, _ := result.LastInsertId()
	action := "created player"
	details := fmt.Sprintf("Created player %q", name)
	if email != "" {
		details += fmt.Sprintf(" with email %s", email)
	}
	if isAdmin {
		action = "created admin"
	}
	app.logAdminActionFromRequest(r, action, "player", int(playerID), details)

	app.redirectWithReturn(w, r, "/admin", "Player+created")
}

func (app *App) updatePlayer(w http.ResponseWriter, r *http.Request) {
	playerID, err := parseEntityID(r.URL.Path, "/admin/players/", "update")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		app.serverError(w, err)
		return
	}

	name := strings.TrimSpace(r.FormValue("name"))
	email := strings.TrimSpace(strings.ToLower(r.FormValue("email")))
	teamID, _ := strconv.Atoi(r.FormValue("team_id"))
	isAdmin := r.FormValue("is_admin") == "on"
	password := r.FormValue("password")

	if name == "" || teamID == 0 {
		app.redirectWithReturn(w, r, "/admin", "Player+name+and+team+are+required")
		return
	}
	if email != "" {
		if _, err := mail.ParseAddress(email); err != nil {
			app.redirectWithReturn(w, r, "/admin", "Enter+a+valid+email+address")
			return
		}
	}
	if isAdmin && email == "" {
		app.redirectWithReturn(w, r, "/admin", "Admin+accounts+need+an+email+for+login")
		return
	}

	var currentStatus string
	if err := app.db.QueryRow(`SELECT COALESCE(admin_status, 'none') FROM players WHERE id = ?`, playerID).Scan(&currentStatus); err != nil {
		app.redirectWithReturn(w, r, "/admin", "Player+not+found")
		return
	}
	adminStatus := "none"
	switch {
	case isAdmin && currentStatus == "pending":
		adminStatus = "pending"
	case isAdmin:
		adminStatus = "approved"
	}

	var emailValue any
	if email != "" {
		emailValue = email
	}

	_, err = app.db.Exec(
		`UPDATE players SET name = ?, email = ?, team_id = ?, is_admin = ?, admin_status = ? WHERE id = ?`,
		name, emailValue, teamID, isAdmin, adminStatus, playerID,
	)
	if err != nil {
		app.redirectWithReturn(w, r, "/admin", "Could+not+update+player")
		return
	}
	if password != "" {
		hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if err != nil {
			app.serverError(w, err)
			return
		}
		if _, err := app.db.Exec(`UPDATE players SET password_hash = ? WHERE id = ?`, string(hash), playerID); err != nil {
			app.redirectWithReturn(w, r, "/admin", "Could+not+update+password")
			return
		}
	}
	app.logAdminActionFromRequest(r, "updated player", "player", playerID, fmt.Sprintf("Updated player %q", name))

	app.redirectWithReturn(w, r, "/admin", "Player+updated")
}

func (app *App) approveAdmin(w http.ResponseWriter, r *http.Request) {
	playerID, err := parseEntityID(r.URL.Path, "/admin/players/", "approve-admin")
	if err != nil {
		http.NotFound(w, r)
		return
	}

	var name, email, status string
	if err := app.db.QueryRow(`SELECT name, COALESCE(email, ''), COALESCE(admin_status, 'none') FROM players WHERE id = ?`, playerID).Scan(&name, &email, &status); err != nil {
		app.redirectWithReturn(w, r, "/admin", "Admin+request+not+found")
		return
	}
	if status != "pending" {
		app.redirectWithReturn(w, r, "/admin", "That+account+is+not+waiting+for+approval")
		return
	}

	session, _ := app.currentSession(r)
	if _, err := app.db.Exec(
		`UPDATE players SET is_admin = 1, admin_status = 'approved', approved_by = ?, approved_at = CURRENT_TIMESTAMP WHERE id = ?`,
		session.PlayerID, playerID,
	); err != nil {
		app.redirectWithReturn(w, r, "/admin", "Could+not+approve+that+admin")
		return
	}

	app.logAdminActionFromRequest(r, "approved admin", "player", playerID, fmt.Sprintf("Approved pending admin %q with email %s", name, email))
	app.redirectWithReturn(w, r, "/admin", "Admin+approved")
}

func (app *App) createGame(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		app.serverError(w, err)
		return
	}

	gameType := strings.TrimSpace(r.FormValue("game_type"))
	if gameType != "singles" && gameType != "doubles" {
		app.redirectWithReturn(w, r, "/admin", "Choose+a+valid+game+type")
		return
	}

	team1ID, _ := strconv.Atoi(r.FormValue("team1_id"))
	team2ID, _ := strconv.Atoi(r.FormValue("team2_id"))
	t1p1, _ := strconv.Atoi(r.FormValue("team1_player1_id"))
	t1p2, _ := strconv.Atoi(r.FormValue("team1_player2_id"))
	t2p1, _ := strconv.Atoi(r.FormValue("team2_player1_id"))
	t2p2, _ := strconv.Atoi(r.FormValue("team2_player2_id"))
	refereeID, _ := strconv.Atoi(r.FormValue("referee_id"))
	scheduledAt, err := parseDateTimeLocal(r.FormValue("scheduled_date"))
	if err != nil {
		app.redirectWithReturn(w, r, "/admin", "Provide+a+valid+scheduled+date")
		return
	}
	session, _ := app.currentSession(r)

	if err := validateGame(gameType, team1ID, team2ID, t1p1, t1p2, t2p1, t2p2); err != nil {
		app.redirectWithReturn(w, r, "/admin", urlSafe(err.Error()))
		return
	}

	if gameType == "singles" {
		t1p2 = 0
		t2p2 = 0
	}

		result, err := app.db.Exec(
		`INSERT INTO games (
			game_type, team1_id, team2_id, team1_player1_id, team1_player2_id,
			team2_player1_id, team2_player2_id, referee_id, scheduled_date, created_by
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		gameType, team1ID, team2ID, nullableInt(t1p1), nullableInt(t1p2), nullableInt(t2p1), nullableInt(t2p2), nullableInt(refereeID), scheduledAt.Format(time.RFC3339), session.PlayerID,
	)
	if err != nil {
		app.redirectWithReturn(w, r, "/admin", "Could+not+create+game")
		return
	}
	gameID, _ := result.LastInsertId()
	if err := app.createDefaultRoleEvents(int(gameID), gameType, team1ID, team2ID, t1p1, t1p2, t2p1, t2p2); err != nil {
		app.serverError(w, err)
		return
	}
	app.logAdminActionFromRequest(r, "scheduled game", "game", int(gameID), fmt.Sprintf("Scheduled %s: %d vs %d on %s", gameType, team1ID, team2ID, scheduledAt.Format(time.RFC3339)))

	app.redirectWithReturn(w, r, "/admin", "Game+scheduled")
}

func (app *App) generateRefereeCode(w http.ResponseWriter, r *http.Request) {
	gameID, err := parseGameID(r.URL.Path, "referee-code")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	session, _ := app.currentSession(r)
	code, err := randomAccessCode()
	if err != nil {
		app.serverError(w, err)
		return
	}
	expiresAt := time.Now().Add(12 * time.Hour).Format(time.RFC3339)
	if _, err := app.db.Exec(
		`INSERT INTO referee_codes (game_id, code, issued_by, expires_at) VALUES (?, ?, ?, ?)`,
		gameID, code, session.PlayerID, expiresAt,
	); err != nil {
		app.serverError(w, err)
		return
	}
	app.logAdminActionFromRequest(r, "generated referee code", "game", gameID, fmt.Sprintf("Issued referee code %s for game #%d", code, gameID))
	app.redirectWithReturn(w, r, "/admin", "Referee+code+"+code+"+created")
}

func (app *App) refereeAccess(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		app.serverError(w, err)
		return
	}
	gameID, _ := strconv.Atoi(r.FormValue("game_id"))
	name := strings.TrimSpace(r.FormValue("name"))
	code := strings.TrimSpace(r.FormValue("code"))
	if gameID == 0 || name == "" || code == "" {
		app.redirectWithReturn(w, r, "/schedule", "Enter+your+name+and+the+referee+code")
		return
	}

	var expectedCode string
	err := app.db.QueryRow(`
		SELECT code
		FROM referee_codes
		WHERE game_id = ?
		  AND (expires_at IS NULL OR expires_at = '' OR expires_at >= ?)
		ORDER BY id DESC
		LIMIT 1
	`, gameID, time.Now().Format(time.RFC3339)).Scan(&expectedCode)
	if err != nil || expectedCode != code {
		app.redirectWithReturn(w, r, "/schedule", "That+referee+code+is+not+valid")
		return
	}
	if err := app.createRefereeSession(w, RefereeSession{
		GameID:  gameID,
		Name:    name,
		Expires: time.Now().Add(12 * time.Hour),
	}); err != nil {
		app.serverError(w, err)
		return
	}
	app.redirectWithReturn(w, r, "/schedule", "Referee+access+enabled+for+"+urlSafe(name))
}

func (app *App) refereeRoutes(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/goal"):
		app.recordGoalByReferee(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/lineup"):
		app.updateGameLineupByReferee(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/minute"):
		app.updateRefereeMinute(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/pause"):
		app.pauseRefereeGame(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/resume"):
		app.resumeRefereeGame(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (app *App) updateGameLineupAsAdmin(w http.ResponseWriter, r *http.Request) {
	gameID, err := parseGameID(r.URL.Path, "lineup")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	session, _ := app.currentSession(r)
	app.updateGameLineup(w, r, gameID, session.PlayerID, session.Name, false)
}

func (app *App) updateGameLineupByReferee(w http.ResponseWriter, r *http.Request) {
	gameID, err := parsePublicGameID(r.URL.Path, "lineup")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	refSession, ok := app.currentRefereeSession(r, gameID)
	if !ok {
		app.redirectWithReturn(w, r, "/schedule", "Referee+access+is+required")
		return
	}
	app.updateGameLineup(w, r, gameID, 0, refSession.Name, true)
}

func (app *App) updateGameLineup(w http.ResponseWriter, r *http.Request, gameID int, recordedBy int, recordedByName string, publicRef bool) {
	if err := r.ParseForm(); err != nil {
		app.serverError(w, err)
		return
	}
	minute, _ := strconv.Atoi(r.FormValue("minute"))
	team1Striker, _ := strconv.Atoi(r.FormValue("team1_striker_id"))
	team1Defender, _ := strconv.Atoi(r.FormValue("team1_defender_id"))
	team2Striker, _ := strconv.Atoi(r.FormValue("team2_striker_id"))
	team2Defender, _ := strconv.Atoi(r.FormValue("team2_defender_id"))

	if err := app.applyGameLineup(gameID, minute, team1Striker, team1Defender, team2Striker, team2Defender, recordedBy, recordedByName); err != nil {
		app.redirectWithReturn(w, r, fallbackPath(publicRef), urlSafe(err.Error()))
		return
	}
	if !publicRef {
		app.logAdminActionFromRequest(r, "updated lineup", "game", gameID, fmt.Sprintf("Updated lineup roles for game #%d", gameID))
	}
	app.redirectWithReturn(w, r, fallbackPath(publicRef), "Lineup+updated")
}

func (app *App) recordGoalByReferee(w http.ResponseWriter, r *http.Request) {
	gameID, err := parsePublicGameID(r.URL.Path, "goal")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if _, ok := app.currentRefereeSession(r, gameID); !ok {
		app.redirectWithReturn(w, r, "/schedule", "Referee+access+is+required")
		return
	}
	app.recordGoalForContext(w, r, gameID, 0, true)
}

func (app *App) updateRefereeMinute(w http.ResponseWriter, r *http.Request) {
	gameID, err := parsePublicGameID(r.URL.Path, "minute")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if _, ok := app.currentRefereeSession(r, gameID); !ok {
		app.redirectWithReturn(w, r, "/schedule", "Referee+access+is+required")
		return
	}
	if err := r.ParseForm(); err != nil {
		app.serverError(w, err)
		return
	}
	minute, err := strconv.Atoi(r.FormValue("minute"))
	if err != nil || minute < 0 {
		app.redirectWithReturn(w, r, "/schedule", "Enter+a+valid+live+minute")
		return
	}
	if _, err := app.db.Exec(`UPDATE games SET live_minute = ?, status = CASE WHEN status = 'scheduled' THEN 'live' ELSE status END WHERE id = ?`, minute, gameID); err != nil {
		app.serverError(w, err)
		return
	}
	app.redirectWithReturn(w, r, "/schedule", "Live+minute+updated")
}

func (app *App) pauseRefereeGame(w http.ResponseWriter, r *http.Request) {
	gameID, err := parsePublicGameID(r.URL.Path, "pause")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if _, ok := app.currentRefereeSession(r, gameID); !ok {
		app.redirectWithReturn(w, r, "/schedule", "Referee+access+is+required")
		return
	}
	if _, err := app.db.Exec(`UPDATE games SET status = 'live', live_paused = 1 WHERE id = ?`, gameID); err != nil {
		app.serverError(w, err)
		return
	}
	app.redirectWithReturn(w, r, "/schedule", "Match+paused")
}

func (app *App) resumeRefereeGame(w http.ResponseWriter, r *http.Request) {
	gameID, err := parsePublicGameID(r.URL.Path, "resume")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if _, ok := app.currentRefereeSession(r, gameID); !ok {
		app.redirectWithReturn(w, r, "/schedule", "Referee+access+is+required")
		return
	}
	if _, err := app.db.Exec(`UPDATE games SET status = 'live', live_paused = 0 WHERE id = ?`, gameID); err != nil {
		app.serverError(w, err)
		return
	}
	app.redirectWithReturn(w, r, "/schedule", "Match+resumed")
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
		app.redirectWithReturn(w, r, "/admin", "Enter+valid+scores")
		return
	}

	if _, err := app.db.Exec(
		`UPDATE games
		 SET team1_score = ?, team2_score = ?, status = 'played', played_date = ?, live_paused = 0,
		     live_minute = CASE WHEN live_minute > 0 THEN live_minute ELSE 90 END
		 WHERE id = ?`,
		team1Score, team2Score, time.Now().Format(time.RFC3339), gameID,
	); err != nil {
		app.redirectWithReturn(w, r, "/admin", "Could+not+save+result")
		return
	}
	app.logAdminActionFromRequest(r, "updated result", "game", gameID, fmt.Sprintf("Saved result %d-%d", team1Score, team2Score))

	app.redirectWithReturn(w, r, "/admin", "Result+saved")
}

func (app *App) recordGoal(w http.ResponseWriter, r *http.Request) {
	gameID, err := parseGameID(r.URL.Path, "goal")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	app.recordGoalForContext(w, r, gameID, func() int {
		session, _ := app.currentSession(r)
		return session.PlayerID
	}(), false)
}

func (app *App) recordGoalForContext(w http.ResponseWriter, r *http.Request, gameID int, recordedBy int, publicRef bool) {
	if err := r.ParseForm(); err != nil {
		app.serverError(w, err)
		return
	}

	scorerID, err := strconv.Atoi(r.FormValue("scorer_id"))
	if err != nil || scorerID == 0 {
		app.redirectWithReturn(w, r, fallbackPath(publicRef), "Choose+a+valid+scorer")
		return
	}
	minute, _ := strconv.Atoi(r.FormValue("minute"))

	tx, err := app.db.Begin()
	if err != nil {
		app.serverError(w, err)
		return
	}
	defer tx.Rollback()

	var status string
	var team1ID, team2ID int
	var scorerName string
	var team1Players, team2Players [2]int
	if err := tx.QueryRow(`
		SELECT
			COALESCE(status, 'scheduled'),
			team1_id, team2_id,
			COALESCE(team1_player1_id, 0), COALESCE(team1_player2_id, 0),
			COALESCE(team2_player1_id, 0), COALESCE(team2_player2_id, 0)
		FROM games
		WHERE id = ?
	`, gameID).Scan(&status, &team1ID, &team2ID, &team1Players[0], &team1Players[1], &team2Players[0], &team2Players[1]); err != nil {
		app.redirectWithReturn(w, r, fallbackPath(publicRef), "Game+not+found")
		return
	}
	if status == "cancelled" {
		app.redirectWithReturn(w, r, fallbackPath(publicRef), "Cannot+record+a+goal+for+a+cancelled+game")
		return
	}

	scorerTeamID := 0
	for _, playerID := range team1Players {
		if scorerID == playerID {
			scorerTeamID = team1ID
		}
	}
	for _, playerID := range team2Players {
		if scorerID == playerID {
			scorerTeamID = team2ID
		}
	}
	if scorerTeamID == 0 {
		app.redirectWithReturn(w, r, fallbackPath(publicRef), "That+player+is+not+in+this+game")
		return
	}
	if err := tx.QueryRow(`SELECT name FROM players WHERE id = ?`, scorerID).Scan(&scorerName); err != nil {
		app.redirectWithReturn(w, r, fallbackPath(publicRef), "Scorer+not+found")
		return
	}

	if _, err := tx.Exec(
		`INSERT INTO goal_events (game_id, scorer_id, recorded_by, minute) VALUES (?, ?, ?, ?)`,
		gameID, scorerID, nullableInt(recordedBy), minute,
	); err != nil {
		app.serverError(w, err)
		return
	}

	scoreColumn := "team2_score"
	if scorerTeamID == team1ID {
		scoreColumn = "team1_score"
	}
	if minute < 0 {
		minute = 0
	}
	if _, err := tx.Exec(
		fmt.Sprintf(`UPDATE games SET %s = %s + 1, status = 'live', live_paused = 0, live_minute = CASE WHEN ? > live_minute THEN ? ELSE live_minute END WHERE id = ?`, scoreColumn, scoreColumn),
		minute, minute, gameID,
	); err != nil {
		app.serverError(w, err)
		return
	}
	if err := tx.Commit(); err != nil {
		app.serverError(w, err)
		return
	}

	if !publicRef {
		app.logAdminActionFromRequest(r, "recorded goal", "game", gameID, fmt.Sprintf("Recorded goal for %s in game #%d", scorerName, gameID))
	}
	app.redirectWithReturn(w, r, fallbackPath(publicRef), "Goal+recorded")
}

func (app *App) cancelGame(w http.ResponseWriter, r *http.Request) {
	gameID, err := parseGameID(r.URL.Path, "cancel")
	if err != nil {
		http.NotFound(w, r)
		return
	}

	if _, err := app.db.Exec(`UPDATE games SET status = 'cancelled' WHERE id = ?`, gameID); err != nil {
		app.redirectWithReturn(w, r, "/admin", "Could+not+cancel+game")
		return
	}
	app.logAdminActionFromRequest(r, "cancelled game", "game", gameID, "Marked game as cancelled")

	app.redirectWithReturn(w, r, "/admin", "Game+cancelled")
}

func (app *App) updateRules(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		app.serverError(w, err)
		return
	}
	rules := strings.TrimSpace(r.FormValue("rules"))
	if rules == "" {
		app.redirectWithReturn(w, r, "/admin", "Rules+cannot+be+empty")
		return
	}

	if _, err := app.db.Exec(
		`INSERT INTO settings (key, value, updated_at) VALUES ('league_rules', ?, CURRENT_TIMESTAMP)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP`,
		rules,
	); err != nil {
		app.redirectWithReturn(w, r, "/admin", "Could+not+save+rules")
		return
	}
	app.logAdminActionFromRequest(r, "updated rules", "settings", 0, "Updated league rules")

	app.redirectWithReturn(w, r, "/admin", "Rules+updated")
}

func (app *App) updateStatsConfig(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		app.serverError(w, err)
		return
	}
	limit, err := strconv.Atoi(r.FormValue("stats_limit"))
	if err != nil || limit < 1 || limit > 25 {
		app.redirectWithReturn(w, r, "/admin", "Choose+a+stats+limit+between+1+and+25")
		return
	}
	defenderLimit, err := strconv.Atoi(r.FormValue("defender_limit"))
	if err != nil || defenderLimit < 1 || defenderLimit > 25 {
		app.redirectWithReturn(w, r, "/admin", "Choose+a+defender+limit+between+1+and+25")
		return
	}
	statsWidgets := normalizeStatsWidgets(r.FormValue("stats_widgets"))
	if len(statsWidgets) == 0 {
		app.redirectWithReturn(w, r, "/admin", "Choose+at+least+one+stats+widget")
		return
	}
	if _, err := app.db.Exec(
		`INSERT INTO settings (key, value, updated_at) VALUES ('stats_limit', ?, CURRENT_TIMESTAMP)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP`,
		strconv.Itoa(limit),
	); err != nil {
		app.serverError(w, err)
		return
	}
	if _, err := app.db.Exec(
		`INSERT INTO settings (key, value, updated_at) VALUES ('defender_limit', ?, CURRENT_TIMESTAMP)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP`,
		strconv.Itoa(defenderLimit),
	); err != nil {
		app.serverError(w, err)
		return
	}
	if _, err := app.db.Exec(
		`INSERT INTO settings (key, value, updated_at) VALUES ('stats_widgets', ?, CURRENT_TIMESTAMP)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP`,
		strings.Join(statsWidgets, ","),
	); err != nil {
		app.serverError(w, err)
		return
	}
	app.logAdminActionFromRequest(r, "updated stats config", "settings", 0, fmt.Sprintf("Set stats widgets=%s strikers=%d defenders=%d", strings.Join(statsWidgets, ","), limit, defenderLimit))
	app.redirectWithReturn(w, r, "/admin", "Stats+display+updated")
}

func (app *App) createBackup(w http.ResponseWriter, r *http.Request) {
	backupDir := filepath.Join(filepath.Dir(app.dbPath), "backups")
	if err := os.MkdirAll(backupDir, 0o755); err != nil {
		app.serverError(w, err)
		return
	}
	name := "fussball-" + time.Now().In(kenyaLocation).Format("20060102-150405") + ".db"
	targetPath := filepath.Join(backupDir, name)

	src, err := os.Open(app.dbPath)
	if err != nil {
		app.serverError(w, err)
		return
	}
	defer src.Close()

	dst, err := os.Create(targetPath)
	if err != nil {
		app.serverError(w, err)
		return
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		app.serverError(w, err)
		return
	}
	storageURL := "/admin/backups/" + name
	storageKind := "local"
	if remoteURL, err := uploadBackupToRemote(targetPath, name); err != nil {
		app.serverError(w, err)
		return
	} else if remoteURL != "" {
		storageURL = remoteURL
		storageKind = "remote"
	}
	if _, err := app.db.Exec(
		`INSERT INTO backup_records (name, local_path, storage_url, storage_kind) VALUES (?, ?, ?, ?)`,
		name, targetPath, storageURL, storageKind,
	); err != nil {
		app.serverError(w, err)
		return
	}
	app.logAdminActionFromRequest(r, "created backup", "backup", 0, fmt.Sprintf("Created database backup %s", name))
	app.redirectWithReturn(w, r, "/admin", "Backup+created")
}

func (app *App) downloadBackup(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/admin/backups/"))
	if name == "" || strings.Contains(name, "/") {
		http.NotFound(w, r)
		return
	}
	filePath := filepath.Join(filepath.Dir(app.dbPath), "backups", name)
	if _, err := os.Stat(filePath); err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", name))
	http.ServeFile(w, r, filePath)
}

func (app *App) deleteTeam(w http.ResponseWriter, r *http.Request) {
	teamID, err := parseEntityID(r.URL.Path, "/admin/teams/", "delete")
	if err != nil {
		http.NotFound(w, r)
		return
	}

	tx, err := app.db.Begin()
	if err != nil {
		app.serverError(w, err)
		return
	}
	defer tx.Rollback()

	var teamName string
	if err := tx.QueryRow(`SELECT name FROM teams WHERE id = ?`, teamID).Scan(&teamName); err != nil {
		app.redirectWithReturn(w, r, "/admin", "Team+not+found")
		return
	}

	var playerCount, gameCount int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM players WHERE team_id = ?`, teamID).Scan(&playerCount); err != nil {
		app.serverError(w, err)
		return
	}
	if err := tx.QueryRow(`SELECT COUNT(*) FROM games WHERE team1_id = ? OR team2_id = ?`, teamID, teamID).Scan(&gameCount); err != nil {
		app.serverError(w, err)
		return
	}
	if playerCount > 0 || gameCount > 0 {
		app.redirectWithReturn(w, r, "/admin", "Cannot+delete+a+team+that+still+has+players+or+games.+Edit+or+reassign+the+data+first")
		return
	}
	if _, err := tx.Exec(`DELETE FROM teams WHERE id = ?`, teamID); err != nil {
		app.serverError(w, err)
		return
	}
	if err := tx.Commit(); err != nil {
		app.serverError(w, err)
		return
	}

	app.logAdminActionFromRequest(r, "deleted team", "team", teamID, fmt.Sprintf("Deleted empty team %q", teamName))
	app.redirectWithReturn(w, r, "/admin", "Team+deleted")
}

func (app *App) deletePlayer(w http.ResponseWriter, r *http.Request) {
	playerID, err := parseEntityID(r.URL.Path, "/admin/players/", "delete")
	if err != nil {
		http.NotFound(w, r)
		return
	}

	session, _ := app.currentSession(r)
	if session.PlayerID == playerID {
		app.redirectWithReturn(w, r, "/admin", "You+cannot+delete+the+admin+account+you+are+using")
		return
	}

	tx, err := app.db.Begin()
	if err != nil {
		app.serverError(w, err)
		return
	}
	defer tx.Rollback()

	var playerName, playerEmail, adminStatus string
	var targetIsAdmin bool
	if err := tx.QueryRow(`SELECT name, COALESCE(email, ''), is_admin, COALESCE(admin_status, 'none') FROM players WHERE id = ?`, playerID).Scan(&playerName, &playerEmail, &targetIsAdmin, &adminStatus); err != nil {
		app.redirectWithReturn(w, r, "/admin", "Player+not+found")
		return
	}
	if targetIsAdmin && adminStatus == "approved" {
		currentRank, err := app.approvedAdminRank(session.PlayerID)
		if err != nil {
			app.serverError(w, err)
			return
		}
		targetRank, err := app.approvedAdminRank(playerID)
		if err != nil {
			app.serverError(w, err)
			return
		}
		if currentRank == 0 || currentRank > 2 {
			app.redirectWithReturn(w, r, "/admin", "Only+the+first+two+approved+admins+can+delete+other+admins")
			return
		}
		if targetRank == 1 {
			app.redirectWithReturn(w, r, "/admin", "The+first+approved+admin+is+protected+and+cannot+be+deleted")
			return
		}
	}

	if _, err := tx.Exec(`DELETE FROM games WHERE team1_player1_id = ? OR team1_player2_id = ? OR team2_player1_id = ? OR team2_player2_id = ?`, playerID, playerID, playerID, playerID); err != nil {
		app.serverError(w, err)
		return
	}
	if _, err := tx.Exec(`UPDATE players SET approved_by = NULL WHERE approved_by = ?`, playerID); err != nil {
		app.serverError(w, err)
		return
	}
	if _, err := tx.Exec(`DELETE FROM players WHERE id = ?`, playerID); err != nil {
		app.serverError(w, err)
		return
	}
	if err := tx.Commit(); err != nil {
		app.serverError(w, err)
		return
	}

	app.logAdminActionFromRequest(r, "deleted player", "player", playerID, fmt.Sprintf("Deleted player %q with email %s and related games", playerName, playerEmail))
	app.redirectWithReturn(w, r, "/admin", "Player+deleted")
}

func (app *App) deleteGame(w http.ResponseWriter, r *http.Request) {
	gameID, err := parseEntityID(r.URL.Path, "/admin/games/", "delete")
	if err != nil {
		http.NotFound(w, r)
		return
	}

	if _, err := app.db.Exec(`DELETE FROM games WHERE id = ?`, gameID); err != nil {
		app.redirectWithReturn(w, r, "/admin", "Could+not+delete+game")
		return
	}

	app.logAdminActionFromRequest(r, "deleted game", "game", gameID, "Deleted game permanently")
	app.redirectWithReturn(w, r, "/admin", "Game+deleted")
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
	games, err = app.decorateGamesForRequest(r, games)
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
	logs, err := app.listAdminLogs(25)
	if err != nil {
		app.serverError(w, err)
		return
	}
	logQuery := strings.TrimSpace(r.URL.Query().Get("log_q"))
	showLogSearch := len(logs) >= 6
	if logQuery != "" {
		logs = filterLogs(logs, logQuery)
	}
	session, _ := app.currentSession(r)
	currentRank, err := app.approvedAdminRank(session.PlayerID)
	if err != nil {
		app.serverError(w, err)
		return
	}
	players = app.decoratePlayersForAdmin(players, session.PlayerID, currentRank)
	admins := filterApprovedAdmins(players)
	pendingAdmins := filterPendingAdmins(players)
	backups, err := app.listBackups()
	if err != nil {
		app.serverError(w, err)
		return
	}
	statsLimit, err := app.getStatsLimit()
	if err != nil {
		app.serverError(w, err)
		return
	}
	defenderLimit, err := app.getDefenderLimit()
	if err != nil {
		app.serverError(w, err)
		return
	}
	statsWidgets, err := app.getStatsWidgets()
	if err != nil {
		app.serverError(w, err)
		return
	}

	data := app.baseData(r, "Admin Dashboard", "admin_dashboard")
	data.SearchQuery = logQuery
	data.ShowLogSearch = showLogSearch
	data.Dashboard = DashboardView{
		Teams:         teams,
		Players:       players,
		Admins:        admins,
		PendingAdmins: pendingAdmins,
		Games:         games,
		Standings:     standings,
		Rules:         rules,
		Logs:          logs,
		Backups:       backups,
		StatsLimit:    statsLimit,
		DefenderLimit: defenderLimit,
		StatsWidgets:  statsWidgets,
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
			admin_status TEXT DEFAULT 'none',
			approved_by INTEGER,
			approved_at DATETIME,
			password_hash TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (team_id) REFERENCES teams(id),
			FOREIGN KEY (approved_by) REFERENCES players(id)
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
			live_minute INTEGER DEFAULT 0,
			live_paused BOOLEAN DEFAULT 0,
			referee_id INTEGER,
			scheduled_date DATETIME,
			played_date DATETIME,
			created_by INTEGER,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (team1_id) REFERENCES teams(id),
			FOREIGN KEY (team2_id) REFERENCES teams(id),
			FOREIGN KEY (team1_player1_id) REFERENCES players(id),
			FOREIGN KEY (team1_player2_id) REFERENCES players(id),
			FOREIGN KEY (team2_player1_id) REFERENCES players(id),
			FOREIGN KEY (team2_player2_id) REFERENCES players(id),
			FOREIGN KEY (referee_id) REFERENCES players(id)
		);`,
		`CREATE TABLE IF NOT EXISTS goal_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			game_id INTEGER NOT NULL,
			scorer_id INTEGER NOT NULL,
			recorded_by INTEGER,
			minute INTEGER DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (game_id) REFERENCES games(id),
			FOREIGN KEY (scorer_id) REFERENCES players(id),
			FOREIGN KEY (recorded_by) REFERENCES players(id)
		);`,
		`CREATE TABLE IF NOT EXISTS game_role_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			game_id INTEGER NOT NULL,
			team_id INTEGER NOT NULL,
			player_id INTEGER NOT NULL,
			role TEXT NOT NULL,
			start_minute INTEGER DEFAULT 0,
			end_minute INTEGER,
			recorded_by INTEGER,
			recorded_by_name TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (game_id) REFERENCES games(id),
			FOREIGN KEY (team_id) REFERENCES teams(id),
			FOREIGN KEY (player_id) REFERENCES players(id),
			FOREIGN KEY (recorded_by) REFERENCES players(id)
		);`,
		`CREATE TABLE IF NOT EXISTS referee_codes (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			game_id INTEGER NOT NULL,
			code TEXT NOT NULL,
			issued_by INTEGER,
			expires_at DATETIME,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (game_id) REFERENCES games(id),
			FOREIGN KEY (issued_by) REFERENCES players(id)
		);`,
		`CREATE TABLE IF NOT EXISTS settings (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS backup_records (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			local_path TEXT NOT NULL,
			storage_url TEXT NOT NULL,
			storage_kind TEXT NOT NULL DEFAULT 'local',
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS admin_logs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			admin_id INTEGER NOT NULL,
			action TEXT NOT NULL,
			target_type TEXT NOT NULL,
			target_id INTEGER DEFAULT 0,
			details TEXT NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (admin_id) REFERENCES players(id)
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

	for _, statement := range []string{
		`ALTER TABLE players ADD COLUMN admin_status TEXT DEFAULT 'none';`,
		`ALTER TABLE players ADD COLUMN approved_by INTEGER;`,
		`ALTER TABLE players ADD COLUMN approved_at DATETIME;`,
		`ALTER TABLE games ADD COLUMN referee_id INTEGER;`,
		`ALTER TABLE games ADD COLUMN live_minute INTEGER DEFAULT 0;`,
		`ALTER TABLE games ADD COLUMN live_paused BOOLEAN DEFAULT 0;`,
	} {
		if _, err := app.db.Exec(statement); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("init schema migration: %w", err)
		}
	}

	if _, err := app.db.Exec(`UPDATE players SET admin_status = CASE WHEN is_admin = 1 THEN 'approved' ELSE COALESCE(admin_status, 'none') END WHERE admin_status IS NULL OR admin_status = ''`); err != nil {
		return fmt.Errorf("sync admin status: %w", err)
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

	playerSeed := []struct {
		Name     string
		Email    string
		TeamName string
		Admin    bool
		Password string
	}{
		{"Ari Swift", "ari@fussball.local", "Red Lions", false, "player123"},
		{"Tala Reed", "tala@fussball.local", "Red Lions", false, "player123"},
		{"Nora Pace", "nora@fussball.local", "Blue Rockets", false, "player123"},
		{"Leo Kane", "leo@fussball.local", "Blue Rockets", false, "player123"},
		{"Sami Volt", "sami@fussball.local", "Golden Boots", false, "player123"},
		{"Ivy Cross", "ivy@fussball.local", "Golden Boots", false, "player123"},
		{"Noah Edge", "noah@fussball.local", "Night Owls", false, "player123"},
		{"Zuri Field", "zuri@fussball.local", "Night Owls", false, "player123"},
	}

	playerIDs := make(map[string]int)
	for _, p := range playerSeed {
		hash, err := bcrypt.GenerateFromPassword([]byte(p.Password), bcrypt.DefaultCost)
		if err != nil {
			return err
		}
		adminStatus := "none"
		if p.Admin {
			adminStatus = "approved"
		}
		result, err := tx.Exec(
			`INSERT INTO players (name, email, team_id, is_admin, admin_status, password_hash) VALUES (?, ?, ?, ?, ?, ?)`,
			p.Name, p.Email, teamIDs[p.TeamName], p.Admin, adminStatus, string(hash),
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
		{"singles", "Red Lions", "Blue Rockets", "Ari Swift", "", "Nora Pace", "", "played", 4, 2, now.AddDate(0, 0, -7), now.AddDate(0, 0, -7)},
		{"doubles", "Golden Boots", "Night Owls", "Sami Volt", "Ivy Cross", "Noah Edge", "Zuri Field", "played", 3, 3, now.AddDate(0, 0, -3), now.AddDate(0, 0, -3)},
		{"singles", "Blue Rockets", "Golden Boots", "Leo Kane", "", "Sami Volt", "", "scheduled", 0, 0, now.AddDate(0, 0, 2), time.Time{}},
		{"doubles", "Night Owls", "Red Lions", "Noah Edge", "Zuri Field", "Ari Swift", "Tala Reed", "scheduled", 0, 0, now.AddDate(0, 0, 5), time.Time{}},
	}

	for _, game := range games {
		var playedAt any
		if !game.Played.IsZero() {
			playedAt = game.Played.Format(time.RFC3339)
		}
		result, err := tx.Exec(
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
			nil,
		)
		if err != nil {
			return err
		}
		gameID, _ := result.LastInsertId()
		if err := seedDefaultRoleEvents(tx, int(gameID), game.GameType, teamIDs[game.Team1], teamIDs[game.Team2], playerIDs[game.T1P1], playerIDs[game.T1P2], playerIDs[game.T2P1], playerIDs[game.T2P2]); err != nil {
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
	if _, err := tx.Exec(`INSERT INTO settings (key, value) VALUES ('stats_limit', '10')`); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO settings (key, value) VALUES ('defender_limit', '10')`); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO settings (key, value) VALUES ('stats_widgets', ?)`, strings.Join(defaultStatsWidgets(), ",")); err != nil {
		return err
	}

	return tx.Commit()
}

func (app *App) listGames(whereClause, orderClause string, limit int) ([]GameView, error) {
	query := `
		SELECT
			g.id, g.game_type, g.status,
			COALESCE(g.live_minute, 0), COALESCE(g.live_paused, 0),
			g.team1_id, g.team2_id,
			t1.name, t2.name,
			COALESCE(g.team1_player1_id, 0), COALESCE(p11.name, ''), COALESCE(g.team1_player2_id, 0), COALESCE(p12.name, ''),
			COALESCE(g.team2_player1_id, 0), COALESCE(p21.name, ''), COALESCE(g.team2_player2_id, 0), COALESCE(p22.name, ''),
			g.team1_score, g.team2_score,
			COALESCE(g.referee_id, 0), COALESCE(ref.name, ''),
			COALESCE(g.scheduled_date, ''), COALESCE(g.played_date, '')
		FROM games g
		JOIN teams t1 ON t1.id = g.team1_id
		JOIN teams t2 ON t2.id = g.team2_id
		LEFT JOIN players p11 ON p11.id = g.team1_player1_id
		LEFT JOIN players p12 ON p12.id = g.team1_player2_id
		LEFT JOIN players p21 ON p21.id = g.team2_player1_id
		LEFT JOIN players p22 ON p22.id = g.team2_player2_id
		LEFT JOIN players ref ON ref.id = g.referee_id
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
			&g.LiveMinute, &g.LivePaused,
			&g.Team1ID, &g.Team2ID,
			&g.Team1Name, &g.Team2Name,
			&g.Team1Player1ID, &g.Team1Player1Name, &g.Team1Player2ID, &g.Team1Player2Name,
			&g.Team2Player1ID, &g.Team2Player1Name, &g.Team2Player2ID, &g.Team2Player2Name,
			&g.Team1Score, &g.Team2Score,
			&g.RefereeID, &g.RefereeName,
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
		if s.GamesPlayed >= 4 {
			s.ShowWinRate = true
			s.WinPercentage = fmt.Sprintf("%.1f%%", float64(s.Wins)/float64(s.GamesPlayed)*100)
		} else {
			s.WinPercentage = ""
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
		SELECT p.id, p.name, COALESCE(p.email, ''), COALESCE(p.team_id, 0), COALESCE(t.name, ''), p.is_admin, COALESCE(p.admin_status, 'none')
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
		if err := rows.Scan(&p.ID, &p.Name, &p.Email, &p.TeamID, &p.Team, &p.IsAdmin, &p.AdminStatus); err != nil {
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
		bestDefense := standings[0]
		for _, standing := range standings[1:] {
			if standing.GoalsFor > bestAttack.GoalsFor {
				bestAttack = standing
			}
			if standing.GoalsAgainst < bestDefense.GoalsAgainst {
				bestDefense = standing
			}
		}
		stats.BestAttack = bestAttack.Name
		stats.BestDefense = bestDefense.Name
	}

	if err := app.db.QueryRow(`SELECT COUNT(*) FROM games WHERE status = 'played'`).Scan(&stats.PlayedGamesCount); err != nil {
		return stats, err
	}
	if err := app.db.QueryRow(`SELECT COUNT(*) FROM games WHERE status IN ('scheduled', 'live')`).Scan(&stats.ScheduledGamesCount); err != nil {
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
	if err != nil {
		return stats, err
	}
	statsLimit, err := app.getStatsLimit()
	if err != nil {
		return stats, err
	}
	defenderLimit, err := app.getDefenderLimit()
	if err != nil {
		return stats, err
	}

	stats.TopScorers, err = app.loadStrikerLeaders(statsLimit)
	if err != nil {
		return stats, err
	}
	if len(stats.TopScorers) > 0 {
		stats.TopScorer = stats.TopScorers[0].Name
		stats.TopScorerGoals = stats.TopScorers[0].Value
	}
	stats.TopDefenders, err = app.loadDefenderLeaders(defenderLimit)
	if err != nil {
		return stats, err
	}

	var topGame GameView
	games, err := app.listGames("WHERE g.status = 'played'", "ORDER BY (g.team1_score + g.team2_score) DESC, g.played_date DESC", 1)
	if err != nil {
		return stats, err
	}
	if len(games) > 0 {
		topGame = games[0]
		stats.TopScoringGame = topGame.Team1Name + " vs " + topGame.Team2Name
		stats.TopScoringGoals = topGame.Team1Score + topGame.Team2Score
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

func (app *App) getStatsLimit() (int, error) {
	var raw string
	err := app.db.QueryRow(`SELECT value FROM settings WHERE key = 'stats_limit'`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return 10, nil
	}
	if err != nil {
		return 0, err
	}
	limit, convErr := strconv.Atoi(strings.TrimSpace(raw))
	if convErr != nil || limit < 1 || limit > 25 {
		return 10, nil
	}
	return limit, nil
}

func (app *App) getDefenderLimit() (int, error) {
	var raw string
	err := app.db.QueryRow(`SELECT value FROM settings WHERE key = 'defender_limit'`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return 10, nil
	}
	if err != nil {
		return 0, err
	}
	limit, convErr := strconv.Atoi(strings.TrimSpace(raw))
	if convErr != nil || limit < 1 || limit > 25 {
		return 10, nil
	}
	return limit, nil
}

func (app *App) getStatsWidgets() ([]string, error) {
	var raw string
	err := app.db.QueryRow(`SELECT value FROM settings WHERE key = 'stats_widgets'`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return defaultStatsWidgets(), nil
	}
	if err != nil {
		return nil, err
	}
	widgets := normalizeStatsWidgets(raw)
	if len(widgets) == 0 {
		return defaultStatsWidgets(), nil
	}
	return widgets, nil
}

func defaultStatsWidgets() []string {
	return []string{
		"top_team",
		"best_attack",
		"best_defense",
		"top_scorer",
		"played_vs_scheduled",
		"highest_scoring_game",
		"top_scorers_table",
		"top_defenders_table",
	}
}

func normalizeStatsWidgets(raw string) []string {
	allowed := map[string]bool{
		"top_team": true,
		"best_attack": true,
		"best_defense": true,
		"top_scorer": true,
		"most_active_player": true,
		"played_vs_scheduled": true,
		"highest_scoring_game": true,
		"top_scorers_table": true,
		"top_defenders_table": true,
	}
	parts := strings.FieldsFunc(strings.ToLower(raw), func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r' || r == '\t' || r == ' '
	})
	seen := make(map[string]bool)
	var widgets []string
	for _, part := range parts {
		if !allowed[part] || seen[part] {
			continue
		}
		seen[part] = true
		widgets = append(widgets, part)
	}
	return widgets
}

func (app *App) loadStrikerLeaders(limit int) ([]PlayerStat, error) {
	rows, err := app.db.Query(`
		SELECT p.name, COALESCE(t.name, ''), COALESCE(goals.goals, 0), COALESCE(minutes.minutes, 0)
		FROM players p
		LEFT JOIN teams t ON t.id = p.team_id
		LEFT JOIN (
			SELECT ge.scorer_id AS player_id, COUNT(*) AS goals
			FROM goal_events ge
			JOIN game_role_events gre
				ON gre.game_id = ge.game_id
				AND gre.player_id = ge.scorer_id
				AND gre.role = 'striker'
				AND ge.minute >= gre.start_minute
				AND (gre.end_minute IS NULL OR ge.minute < gre.end_minute)
			GROUP BY ge.scorer_id
		) goals ON goals.player_id = p.id
		LEFT JOIN (
			SELECT gre.player_id,
				SUM(
					(CASE
						WHEN gre.end_minute IS NULL THEN
							CASE
								WHEN g.status = 'played' AND g.live_minute < 90 THEN 90
								ELSE g.live_minute
							END
						ELSE gre.end_minute
					END) - gre.start_minute
				) AS minutes
			FROM game_role_events gre
			JOIN games g ON g.id = gre.game_id
			WHERE gre.role = 'striker'
			GROUP BY gre.player_id
		) minutes ON minutes.player_id = p.id
		WHERE COALESCE(goals.goals, 0) > 0 OR COALESCE(minutes.minutes, 0) > 0
		ORDER BY COALESCE(goals.goals, 0) DESC, COALESCE(minutes.minutes, 0) DESC, p.name ASC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var leaders []PlayerStat
	for rows.Next() {
		var item PlayerStat
		if err := rows.Scan(&item.Name, &item.Team, &item.Value, &item.Minutes); err != nil {
			return nil, err
		}
		leaders = append(leaders, item)
	}
	return leaders, rows.Err()
}

func (app *App) loadDefenderLeaders(limit int) ([]PlayerStat, error) {
	rows, err := app.db.Query(`
		SELECT p.name, COALESCE(t.name, ''), COALESCE(def.conceded, 0), COALESCE(minutes.minutes, 0)
		FROM players p
		LEFT JOIN teams t ON t.id = p.team_id
		LEFT JOIN (
			SELECT gre.player_id, COUNT(ge.id) AS conceded
			FROM game_role_events gre
			JOIN games g ON g.id = gre.game_id
			LEFT JOIN goal_events ge
				ON ge.game_id = gre.game_id
				AND ge.minute >= gre.start_minute
				AND (gre.end_minute IS NULL OR ge.minute < gre.end_minute)
				AND (
					(gre.team_id = g.team1_id AND ge.scorer_id IN (COALESCE(g.team2_player1_id, 0), COALESCE(g.team2_player2_id, 0)))
					OR
					(gre.team_id = g.team2_id AND ge.scorer_id IN (COALESCE(g.team1_player1_id, 0), COALESCE(g.team1_player2_id, 0)))
				)
			WHERE gre.role = 'defender'
			GROUP BY gre.player_id
		) def ON def.player_id = p.id
		LEFT JOIN (
			SELECT gre.player_id,
				SUM(
					(CASE
						WHEN gre.end_minute IS NULL THEN
							CASE
								WHEN g.status = 'played' AND g.live_minute < 90 THEN 90
								ELSE g.live_minute
							END
						ELSE gre.end_minute
					END) - gre.start_minute
				) AS minutes
			FROM game_role_events gre
			JOIN games g ON g.id = gre.game_id
			WHERE gre.role = 'defender'
			GROUP BY gre.player_id
		) minutes ON minutes.player_id = p.id
		WHERE COALESCE(def.conceded, 0) > 0 OR COALESCE(minutes.minutes, 0) > 0
		ORDER BY COALESCE(def.conceded, 0) ASC, COALESCE(minutes.minutes, 0) DESC, p.name ASC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var leaders []PlayerStat
	for rows.Next() {
		var item PlayerStat
		if err := rows.Scan(&item.Name, &item.Team, &item.Value, &item.Minutes); err != nil {
			return nil, err
		}
		leaders = append(leaders, item)
	}
	return leaders, rows.Err()
}

func (app *App) listAdminLogs(limit int) ([]AdminLog, error) {
	query := `
		SELECT l.id, COALESCE(p.name, 'Deleted admin'), l.action, l.target_type, l.details, COALESCE(l.created_at, '')
		FROM admin_logs l
		LEFT JOIN players p ON p.id = l.admin_id
		ORDER BY l.created_at DESC, l.id DESC
	`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := app.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []AdminLog
	for rows.Next() {
		var logEntry AdminLog
		var createdRaw string
		if err := rows.Scan(&logEntry.ID, &logEntry.AdminName, &logEntry.Action, &logEntry.Target, &logEntry.Details, &createdRaw); err != nil {
			return nil, err
		}
		logEntry.CreatedAt = formatDateTime(createdRaw)
		logs = append(logs, logEntry)
	}

	return logs, rows.Err()
}

func (app *App) logAdminAction(adminID int, action, targetType string, targetID int, details string) {
	if adminID == 0 {
		return
	}
	if _, err := app.db.Exec(
		`INSERT INTO admin_logs (admin_id, action, target_type, target_id, details) VALUES (?, ?, ?, ?, ?)`,
		adminID, action, targetType, targetID, details,
	); err != nil {
		log.Printf("admin log error: %v", err)
	}
}

func (app *App) logAdminActionFromRequest(r *http.Request, action, targetType string, targetID int, details string) {
	session, ok := app.currentSession(r)
	if !ok || !session.IsAdmin {
		return
	}
	app.logAdminAction(session.PlayerID, action, targetType, targetID, details)
}

func (app *App) approvedAdminRank(playerID int) (int, error) {
	if playerID == 0 {
		return 0, nil
	}

	rows, err := app.db.Query(`
		SELECT id
		FROM players
		WHERE is_admin = 1 AND COALESCE(admin_status, 'none') = 'approved'
		ORDER BY created_at ASC, id ASC
	`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	rank := 0
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		rank++
		if id == playerID {
			return rank, nil
		}
	}

	return 0, rows.Err()
}

func (app *App) decoratePlayersForAdmin(players []Player, currentAdminID, currentAdminRank int) []Player {
	for i := range players {
		player := &players[i]
		if player.IsAdmin && player.AdminStatus == "approved" {
			player.AdminRank, _ = app.approvedAdminRank(player.ID)
		}
		if player.AdminStatus == "pending" {
			player.CanApprove = true
			player.CanDelete = true
			player.ProtectionLabel = "Pending approval"
			continue
		}
		if player.IsAdmin && player.AdminStatus == "approved" {
			switch {
			case player.ID == currentAdminID:
				player.ProtectionLabel = "Current admin"
			case player.AdminRank == 1:
				player.ProtectionLabel = "Protected first admin"
			case currentAdminRank > 0 && currentAdminRank <= 2:
				player.CanDelete = true
				player.ProtectionLabel = "Deletable by first two admins"
			default:
				player.ProtectionLabel = "Only the first two admins can delete approved admins"
			}
			continue
		}
		player.CanDelete = true
	}
	return players
}

func filterApprovedAdmins(players []Player) []Player {
	admins := make([]Player, 0)
	for _, player := range players {
		if player.IsAdmin && player.AdminStatus == "approved" {
			admins = append(admins, player)
		}
	}
	return admins
}

func filterPendingAdmins(players []Player) []Player {
	pending := make([]Player, 0)
	for _, player := range players {
		if player.AdminStatus == "pending" {
			pending = append(pending, player)
		}
	}
	return pending
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

func parseEntityID(path, prefix, action string) (int, error) {
	trimmed := strings.TrimPrefix(path, prefix)
	trimmed = strings.TrimSuffix(trimmed, "/"+action)
	return strconv.Atoi(trimmed)
}

func parseGameID(path, action string) (int, error) {
	trimmed := strings.TrimPrefix(path, "/admin/games/")
	trimmed = strings.TrimSuffix(trimmed, "/"+action)
	return strconv.Atoi(trimmed)
}

func parsePublicGameID(path, action string) (int, error) {
	trimmed := strings.TrimPrefix(path, "/referee/games/")
	trimmed = strings.TrimSuffix(trimmed, "/"+action)
	return strconv.Atoi(trimmed)
}

func parseDateTimeLocal(value string) (time.Time, error) {
	return time.ParseInLocation("2006-01-02T15:04", value, kenyaLocation)
}

func formatDateTime(raw string) string {
	if raw == "" {
		return "TBD"
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02 15:04"} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.In(kenyaLocation).Format("02 Jan 2006, 15:04 EAT")
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

func fallbackPath(publicRef bool) string {
	if publicRef {
		return "/schedule"
	}
	return "/admin"
}

func filterGames(games []GameView, query string) []GameView {
	needle := strings.ToLower(strings.TrimSpace(query))
	if needle == "" {
		return games
	}

	filtered := make([]GameView, 0, len(games))
	for _, game := range games {
		haystack := strings.ToLower(strings.Join([]string{
			game.Team1Name,
			game.Team2Name,
			game.Team1Player1Name,
			game.Team1Player2Name,
			game.Team2Player1Name,
			game.Team2Player2Name,
			game.Status,
			game.ScheduledAt,
			game.PlayedAt,
			game.PlayableDescription,
		}, " "))
		if strings.Contains(haystack, needle) {
			filtered = append(filtered, game)
		}
	}
	return filtered
}

func filterTeams(teams []TeamWithPlayers, query string) []TeamWithPlayers {
	needle := strings.ToLower(strings.TrimSpace(query))
	if needle == "" {
		return teams
	}

	filtered := make([]TeamWithPlayers, 0, len(teams))
	for _, team := range teams {
		var playerBits []string
		for _, player := range team.Players {
			playerBits = append(playerBits, player.Name, player.Email)
		}
		if strings.Contains(strings.ToLower(team.Name+" "+strings.Join(playerBits, " ")), needle) {
			filtered = append(filtered, team)
		}
	}
	return filtered
}

func filterLogs(logs []AdminLog, query string) []AdminLog {
	needle := strings.ToLower(strings.TrimSpace(query))
	if needle == "" {
		return logs
	}

	filtered := make([]AdminLog, 0, len(logs))
	for _, logEntry := range logs {
		haystack := strings.ToLower(strings.Join([]string{
			logEntry.AdminName,
			logEntry.Action,
			logEntry.Target,
			logEntry.Details,
			logEntry.CreatedAt,
		}, " "))
		if strings.Contains(haystack, needle) {
			filtered = append(filtered, logEntry)
		}
	}
	return filtered
}

func filterGamesByStatus(games []GameView, status string) []GameView {
	filtered := make([]GameView, 0)
	for _, game := range games {
		if game.Status == status {
			filtered = append(filtered, game)
		}
	}
	return filtered
}

func (app *App) hasApprovedAdmin() (bool, error) {
	var count int
	if err := app.db.QueryRow(`SELECT COUNT(*) FROM players WHERE is_admin = 1 AND COALESCE(admin_status, 'none') = 'approved'`).Scan(&count); err != nil {
		return false, fmt.Errorf("count approved admins: %w", err)
	}
	return count > 0, nil
}

func (app *App) cleanupOrphanedAdminReferences() error {
	if _, err := app.db.Exec(`
		UPDATE players
		SET approved_by = NULL
		WHERE approved_by IS NOT NULL
		  AND NOT EXISTS (
			SELECT 1
			FROM players approver
			WHERE approver.id = players.approved_by
		  )
	`); err != nil {
		return fmt.Errorf("clear orphaned admin approvals: %w", err)
	}
	return nil
}

func (app *App) cleanupLegacyDefaultAdmin() error {
	var approvedNonLegacy int
	if err := app.db.QueryRow(`
		SELECT COUNT(*)
		FROM players
		WHERE is_admin = 1
		  AND COALESCE(admin_status, 'none') = 'approved'
		  AND lower(email) <> 'admin@fussball.local'
	`).Scan(&approvedNonLegacy); err != nil {
		return fmt.Errorf("count non-legacy admins: %w", err)
	}
	if approvedNonLegacy == 0 {
		return nil
	}

	if _, err := app.db.Exec(`DELETE FROM players WHERE lower(email) = 'admin@fussball.local'`); err != nil {
		return fmt.Errorf("delete legacy default admin: %w", err)
	}
	return nil
}

func (app *App) redirectWithReturn(w http.ResponseWriter, r *http.Request, fallbackPath, flash string) {
	destination := strings.TrimSpace(r.FormValue("return_to"))
	if destination == "" {
		destination = strings.TrimSpace(r.URL.Query().Get("return_to"))
	}
	if destination == "" || !strings.HasPrefix(destination, "/") {
		destination = fallbackPath
	}
	http.Redirect(w, r, appendFlash(destination, flash), http.StatusSeeOther)
}

func appendFlash(destination, flash string) string {
	parts := strings.SplitN(destination, "#", 2)
	base := parts[0]
	hash := ""
	if len(parts) == 2 {
		hash = "#" + parts[1]
	}
	separator := "?"
	if strings.Contains(base, "?") {
		separator = "&"
	}
	return base + separator + "flash=" + flash + hash
}

func (app *App) createRefereeSession(w http.ResponseWriter, session RefereeSession) error {
	tokenBytes := make([]byte, 24)
	if _, err := rand.Read(tokenBytes); err != nil {
		return err
	}
	token := hex.EncodeToString(tokenBytes)

	app.mu.Lock()
	app.refereeSessions[token] = session
	app.mu.Unlock()

	http.SetCookie(w, &http.Cookie{
		Name:     "fussball_referee",
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Expires:  session.Expires,
	})
	return nil
}

func (app *App) currentRefereeSession(r *http.Request, gameID int) (RefereeSession, bool) {
	cookie, err := r.Cookie("fussball_referee")
	if err != nil {
		return RefereeSession{}, false
	}

	app.mu.RLock()
	session, ok := app.refereeSessions[cookie.Value]
	app.mu.RUnlock()
	if !ok || session.Expires.Before(time.Now()) || session.GameID != gameID {
		return RefereeSession{}, false
	}
	return session, true
}

func seedDefaultRoleEvents(tx *sql.Tx, gameID int, gameType string, team1ID, team2ID, t1p1, t1p2, t2p1, t2p2 int) error {
	insertRole := func(teamID, playerID int, role string) error {
		if playerID == 0 {
			return nil
		}
		_, err := tx.Exec(
			`INSERT INTO game_role_events (game_id, team_id, player_id, role, start_minute, recorded_by_name) VALUES (?, ?, ?, ?, 0, 'system')`,
			gameID, teamID, playerID, role,
		)
		return err
	}

	if err := insertRole(team1ID, t1p1, "striker"); err != nil {
		return err
	}
	if err := insertRole(team2ID, t2p1, "striker"); err != nil {
		return err
	}
	if gameType == "doubles" {
		if err := insertRole(team1ID, t1p2, "defender"); err != nil {
			return err
		}
		if err := insertRole(team2ID, t2p2, "defender"); err != nil {
			return err
		}
	}
	return nil
}

func (app *App) createDefaultRoleEvents(gameID int, gameType string, team1ID, team2ID, t1p1, t1p2, t2p1, t2p2 int) error {
	tx, err := app.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := seedDefaultRoleEvents(tx, gameID, gameType, team1ID, team2ID, t1p1, t1p2, t2p1, t2p2); err != nil {
		return err
	}
	return tx.Commit()
}

func (app *App) bootstrapRoleEvents() error {
	rows, err := app.db.Query(`
		SELECT id, game_type, team1_id, team2_id,
			COALESCE(team1_player1_id, 0), COALESCE(team1_player2_id, 0),
			COALESCE(team2_player1_id, 0), COALESCE(team2_player2_id, 0)
		FROM games
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var gameID int
		var gameType string
		var team1ID, team2ID, t1p1, t1p2, t2p1, t2p2 int
		if err := rows.Scan(&gameID, &gameType, &team1ID, &team2ID, &t1p1, &t1p2, &t2p1, &t2p2); err != nil {
			return err
		}
		var count int
		if err := app.db.QueryRow(`SELECT COUNT(*) FROM game_role_events WHERE game_id = ?`, gameID).Scan(&count); err != nil {
			return err
		}
		if count > 0 {
			continue
		}
		if err := app.createDefaultRoleEvents(gameID, gameType, team1ID, team2ID, t1p1, t1p2, t2p1, t2p2); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (app *App) applyGameLineup(gameID, minute, team1Striker, team1Defender, team2Striker, team2Defender, recordedBy int, recordedByName string) error {
	if minute < 0 {
		minute = 0
	}
	var gameType string
	var team1ID, team2ID, t1p1, t1p2, t2p1, t2p2 int
	err := app.db.QueryRow(`
		SELECT game_type, team1_id, team2_id,
			COALESCE(team1_player1_id, 0), COALESCE(team1_player2_id, 0),
			COALESCE(team2_player1_id, 0), COALESCE(team2_player2_id, 0)
		FROM games
		WHERE id = ?
	`, gameID).Scan(&gameType, &team1ID, &team2ID, &t1p1, &t1p2, &t2p1, &t2p2)
	if err != nil {
		return fmt.Errorf("game not found")
	}

	if team1Striker == 0 || team2Striker == 0 {
		return fmt.Errorf("each team needs a striker")
	}
	if gameType == "doubles" && (team1Defender == 0 || team2Defender == 0) {
		return fmt.Errorf("each doubles team needs a defender")
	}
	if gameType == "doubles" && (team1Striker == team1Defender || team2Striker == team2Defender) {
		return fmt.Errorf("the same player cannot be both striker and defender")
	}
	if !containsPlayer(team1Striker, t1p1, t1p2) || !containsPlayer(team2Striker, t2p1, t2p2) {
		return fmt.Errorf("selected striker must belong to the correct team")
	}
	if gameType == "doubles" && (!containsPlayer(team1Defender, t1p1, t1p2) || !containsPlayer(team2Defender, t2p1, t2p2)) {
		return fmt.Errorf("selected defender must belong to the correct team")
	}

	tx, err := app.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := upsertRoleEvent(tx, gameID, team1ID, "striker", team1Striker, minute, recordedBy, recordedByName); err != nil {
		return err
	}
	if err := upsertRoleEvent(tx, gameID, team2ID, "striker", team2Striker, minute, recordedBy, recordedByName); err != nil {
		return err
	}
	if gameType == "doubles" {
		if err := upsertRoleEvent(tx, gameID, team1ID, "defender", team1Defender, minute, recordedBy, recordedByName); err != nil {
			return err
		}
		if err := upsertRoleEvent(tx, gameID, team2ID, "defender", team2Defender, minute, recordedBy, recordedByName); err != nil {
			return err
		}
	} else {
		if _, err := tx.Exec(`UPDATE game_role_events SET end_minute = ? WHERE game_id = ? AND role = 'defender' AND end_minute IS NULL`, minute, gameID); err != nil {
			return err
		}
	}

	if _, err := tx.Exec(`UPDATE games SET live_minute = CASE WHEN ? > live_minute THEN ? ELSE live_minute END WHERE id = ?`, minute, minute, gameID); err != nil {
		return err
	}
	return tx.Commit()
}

func upsertRoleEvent(tx *sql.Tx, gameID, teamID int, role string, playerID, minute, recordedBy int, recordedByName string) error {
	var activeID, activePlayerID int
	err := tx.QueryRow(`SELECT id, player_id FROM game_role_events WHERE game_id = ? AND team_id = ? AND role = ? AND end_minute IS NULL ORDER BY id DESC LIMIT 1`, gameID, teamID, role).Scan(&activeID, &activePlayerID)
	if err == nil && activePlayerID == playerID {
		return nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	if _, err := tx.Exec(`UPDATE game_role_events SET end_minute = ? WHERE game_id = ? AND team_id = ? AND role = ? AND end_minute IS NULL`, minute, gameID, teamID, role); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE game_role_events SET end_minute = ? WHERE game_id = ? AND team_id = ? AND player_id = ? AND role <> ? AND end_minute IS NULL`, minute, gameID, teamID, playerID, role); err != nil {
		return err
	}
	_, err = tx.Exec(
		`INSERT INTO game_role_events (game_id, team_id, player_id, role, start_minute, recorded_by, recorded_by_name) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		gameID, teamID, playerID, role, minute, nullableInt(recordedBy), recordedByName,
	)
	return err
}

func containsPlayer(target int, ids ...int) bool {
	for _, id := range ids {
		if id != 0 && id == target {
			return true
		}
	}
	return false
}

func (app *App) decorateGamesForRequest(r *http.Request, games []GameView) ([]GameView, error) {
	codes, err := app.latestRefereeCodes()
	if err != nil {
		return nil, err
	}
	roleMap, err := app.activeRoleAssignments()
	if err != nil {
		return nil, err
	}
	for i := range games {
		games[i].RefereeCode = codes[games[i].ID]
		if refSession, ok := app.currentRefereeSession(r, games[i].ID); ok {
			games[i].RefereeAuthorized = true
			games[i].RefereeSessionName = refSession.Name
		}
		for _, role := range roleMap[games[i].ID] {
			switch {
			case role.TeamID == games[i].Team1ID && role.Role == "striker":
				games[i].Team1StrikerID = role.PlayerID
				games[i].Team1StrikerName = role.PlayerName
			case role.TeamID == games[i].Team1ID && role.Role == "defender":
				games[i].Team1DefenderID = role.PlayerID
				games[i].Team1DefenderName = role.PlayerName
			case role.TeamID == games[i].Team2ID && role.Role == "striker":
				games[i].Team2StrikerID = role.PlayerID
				games[i].Team2StrikerName = role.PlayerName
			case role.TeamID == games[i].Team2ID && role.Role == "defender":
				games[i].Team2DefenderID = role.PlayerID
				games[i].Team2DefenderName = role.PlayerName
			}
		}
	}
	return games, nil
}

type roleAssignment struct {
	GameID     int
	TeamID     int
	PlayerID   int
	PlayerName string
	Role       string
}

func (app *App) activeRoleAssignments() (map[int][]roleAssignment, error) {
	rows, err := app.db.Query(`
		SELECT gre.game_id, gre.team_id, gre.player_id, p.name, gre.role
		FROM game_role_events gre
		JOIN players p ON p.id = gre.player_id
		WHERE gre.end_minute IS NULL
		ORDER BY gre.game_id, gre.team_id, gre.role
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	assignments := make(map[int][]roleAssignment)
	for rows.Next() {
		var item roleAssignment
		if err := rows.Scan(&item.GameID, &item.TeamID, &item.PlayerID, &item.PlayerName, &item.Role); err != nil {
			return nil, err
		}
		assignments[item.GameID] = append(assignments[item.GameID], item)
	}
	return assignments, rows.Err()
}

func (app *App) latestRefereeCodes() (map[int]string, error) {
	rows, err := app.db.Query(`
		SELECT rc.game_id, rc.code
		FROM referee_codes rc
		JOIN (
			SELECT game_id, MAX(id) AS max_id
			FROM referee_codes
			WHERE COALESCE(expires_at, '') = '' OR expires_at >= ?
			GROUP BY game_id
		) latest ON latest.max_id = rc.id
	`, time.Now().Format(time.RFC3339))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	codes := make(map[int]string)
	for rows.Next() {
		var gameID int
		var code string
		if err := rows.Scan(&gameID, &code); err != nil {
			return nil, err
		}
		codes[gameID] = code
	}
	return codes, rows.Err()
}

func randomAccessCode() (string, error) {
	const digits = "0123456789"
	buf := make([]byte, 6)
	raw := make([]byte, 6)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	for i, b := range raw {
		buf[i] = digits[int(b)%len(digits)]
	}
	return string(buf), nil
}

func (app *App) listBackups() ([]BackupEntry, error) {
	rows, err := app.db.Query(`
		SELECT name, storage_url, created_at, local_path
		FROM backup_records
		ORDER BY created_at DESC, id DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var backups []BackupEntry
	for rows.Next() {
		var entry BackupEntry
		var createdRaw, localPath string
		if err := rows.Scan(&entry.Name, &entry.URL, &createdRaw, &localPath); err != nil {
			return nil, err
		}
		info, err := os.Stat(localPath)
		if err != nil {
			continue
		}
		entry.CreatedAt = formatDateTime(createdRaw)
		entry.SizeLabel = fmt.Sprintf("%.1f KB", float64(info.Size())/1024)
		backups = append(backups, BackupEntry{
			Name:      entry.Name,
			URL:       entry.URL,
			CreatedAt: entry.CreatedAt,
			SizeLabel: entry.SizeLabel,
		})
	}
	return backups, rows.Err()
}

func uploadBackupToRemote(localPath, name string) (string, error) {
	template := strings.TrimSpace(os.Getenv("BACKUP_REMOTE_UPLOAD_URL_TEMPLATE"))
	if template == "" {
		return "", nil
	}
	uploadURL := strings.ReplaceAll(template, "{name}", name)
	file, err := os.Open(localPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	req, err := http.NewRequest(http.MethodPut, uploadURL, file)
	if err != nil {
		return "", err
	}
	if bearer := strings.TrimSpace(os.Getenv("BACKUP_REMOTE_AUTH_BEARER")); bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return "", fmt.Errorf("remote backup upload failed: %s %s", resp.Status, strings.TrimSpace(string(body)))
	}

	publicTemplate := strings.TrimSpace(os.Getenv("BACKUP_REMOTE_PUBLIC_URL_TEMPLATE"))
	if publicTemplate != "" {
		return strings.ReplaceAll(publicTemplate, "{name}", name), nil
	}
	return uploadURL, nil
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
