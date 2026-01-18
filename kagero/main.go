package main

import (
	"database/sql"
	//"encoding/json"
	"fmt"
	"github.com/httmako/jote"
	_ "github.com/lib/pq"
	"html/template"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	// "time"
	"context"
	// "os"
	"embed"
	"github.com/ganigeorgiev/fexpr"
)

type Log struct {
	ID       float64
	Ts       string `json:"ts"`
	Host     string `json:"host"`
	NHost    sql.NullString
	Message  string `json:"message"`
	NMessage sql.NullString
	Doc      string `json:"doc"`
}

type Config struct {
	Port                int    `json:"Port"`
	SQLConnectionString string `json:"sqlconnectionstring"`
	SQLMaxConnections   int    `json:"sqlmaxconnections"`
}

/* TODO:
 *  implement time based filtering for specific dates, e.g. ?st=20020101195959&et=2025xxxxx (or 2025.01.01 19:59:59)
 */

var db *sql.DB
var logger *slog.Logger
//go:embed all:templates/*
var templates embed.FS

func main() {
	jote.ProfilingUntilTimeIfSet(30)
	logger = jote.CreateLogger("stdout")

	config := Config{}

	jote.ReadConfigYAML("config.yaml", &config)

	var err error
	db, err = sql.Open("postgres", config.SQLConnectionString)
	jote.Must(err)
	jote.Must(db.Ping())
	db.SetMaxOpenConns(5)

	mux := http.NewServeMux()
	RequestCounter := atomic.Uint64{}
	jote.AddMetrics(mux, "kagero", &RequestCounter)

	tmpl := template.Must(template.ParseFS(templates, "templates/*"))
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		jote.ExecuteTemplate(tmpl, w, "search", jote.H{})
	})

	mux.HandleFunc("GET /search", func(w http.ResponseWriter, r *http.Request) {
		query := r.FormValue("q")
		page := getNumFromRequest(w, r, "p")
		perpage := getNumFromRequest(w, r, "m")
		if page == -1 || perpage == -1 {
			return
		}
		timespan := r.FormValue("t")
		if !IsValidTimespan(timespan) {
			http.Error(w, "ERROR: invalid timespan", 400)
			return
		}
		//timestamps, counts := getRowCountForGraphic(r.Context(), timespan)
		jote.ExecuteTemplate(tmpl, w, "search", jote.H{
			"list":   getRows(r.Context(), query, page, perpage),
			//"bar_ts": timestamps,
			//"bar_c":  counts,
		})
	})

	mux.HandleFunc("GET /view", func(w http.ResponseWriter, r *http.Request) {
		id := getNumFromRequest(w, r, "id")
		if id == -1 || id == 0 {
			return
		}
		jote.ExecuteTemplate(tmpl, w, "view", jote.H{
			"doc": getDoc(r.Context(), id),
		})
	})

	jote.RunMux(":"+strconv.Itoa(config.Port), jote.AddLoggingToMuxWithCounter(mux, logger, &RequestCounter), logger)
}

func IsValidTimespan(timespan string) bool {
	arr := strings.Split(timespan, " ")
	if len(arr) != 2 {
		return false
	}
	if _, err := strconv.Atoi(arr[0]); err != nil {
		return false
	}
	t := arr[1]
	if t != "minutes" && t != "days" && t != "seconds" && t != "hours" {
		return false
	}
	return true
}

func getNumFromRequest(w http.ResponseWriter, r *http.Request, key string) int {
	in := r.FormValue(key)
	ret := 0
	var err error

	if in == "" {
		return 0
	}
	ret, err = strconv.Atoi(in)
	if err != nil {
		http.Error(w, "ERROR: "+key+" is not a number", 400)
		return -1
	}
	if ret < 0 {
		http.Error(w, "ERROR: "+key+" is < 0", 400)
		return -1
	}
	return ret
}

func getDoc(ctx context.Context, id int) Log {
	var log Log
	jote.Must(db.QueryRowContext(ctx, "SELECT id,ts,doc FROM docs WHERE id=$1", id).Scan(&log.ID, &log.Ts, &log.Doc))
	return log
}

func getRows(ctx context.Context, q string, p int, m int) []Log {
	var logs []Log
	rows, err := doSearchSql(ctx, q, p, m)
	jote.Must(err)
	defer rows.Close()
	for rows.Next() {
		log := Log{}
		jote.Must(rows.Scan(&log.ID, &log.Ts, &log.NHost, &log.NMessage))
		if log.NMessage.Valid {
			log.Message = log.NMessage.String
		}
		if log.NHost.Valid {
			log.Host = log.NHost.String
		}
		logs = append(logs, log)
	}
	jote.Must(rows.Err())
	jote.Must(rows.Close())
	return logs
}
/*
var timeList = []string{"seconds", "minutes", "hours", "days"}

func GetDateTrunc(timespan string) string {
	arr := strings.Split(timespan, " ")
	num, _ := strconv.Atoi(arr[0])
	takeSmaller := 0
	if num < 3 {
		takeSmaller = 1
	}
	for i, l := range timeList {
		if l == arr[1] {
			return timeList[max(0, i-takeSmaller)]
		}
	}
	return "hours"
}

func getRowCountForGraphic(ctx context.Context, timespan string) (string, string) {
	var timestamps []string
	var counts []int
	dateTrunc := GetDateTrunc(timespan)
	fmt.Println("SELECT date_trunc('" + dateTrunc + "', ts) AS time_bucket, COUNT(*) AS row_count FROM docs WHERE ts > CURRENT_TIMESTAMP - INTERVAL '" + timespan + "' GROUP BY time_bucket ORDER BY time_bucket")
	rows, err := db.QueryContext(ctx, "SELECT date_trunc('"+dateTrunc+"', ts) AS time_bucket, COUNT(*) AS row_count FROM docs WHERE ts > CURRENT_TIMESTAMP - INTERVAL '"+timespan+"' GROUP BY time_bucket ORDER BY time_bucket")
	jote.Must(err)
	defer rows.Close()
	var ts string
	var c int
	for rows.Next() {
		jote.Must(rows.Scan(&ts, &c))
		timestamps = append(timestamps, ts)
		counts = append(counts, c)
	}
	jote.Must(rows.Err())
	jote.Must(rows.Close())
	tss, err := json.Marshal(timestamps)
	jote.Must(err)
	cs, err := json.Marshal(counts)
	jote.Must(err)
	return string(tss), string(cs)
}
*/

func doSearchSql(ctx context.Context, query string, page int, maxperpage int) (*sql.Rows, error) {
	maxperpage = max(min(maxperpage, 500), 10)
	// page = max(min(page, 5), 0)
	if query == "" {
		return db.QueryContext(ctx, "SELECT id, ts, doc->'_meta'->>'host' as host, doc->>'message' as message FROM docs ORDER BY id DESC LIMIT $1", maxperpage)
	}
	whereClause, args := createSqlWhereClause(query)
	return db.QueryContext(ctx, "SELECT id,ts,doc->'_meta'->>'host', doc->>'message' FROM docs WHERE "+whereClause+" ORDER BY id DESC LIMIT "+strconv.Itoa(maxperpage), args...)
}

// SELECT * FROM docs WHERE ts > '2026-01-08T19:03:03'
// SELECT date_trunc('hour', ts) AS time_bucket, COUNT(*) AS row_count FROM docs GROUP BY time_bucket ORDER BY time_bucket

func createSqlWhereClause(input string) (string, []any) {
	exprGroup, err := fexpr.Parse(input)
	if err != nil {
		panic(err)
	}
	where, args, _ := createSqlWhereClauseLoop(exprGroup, "", []any{}, 1)
	return where, args
}

func createSqlWhereClauseLoop(eg []fexpr.ExprGroup, where string, args []any, argc int) (string, []any, int) {
	for i, e := range eg {
		item := e.Item
		switch i := item.(type) {
		case fexpr.Expr:
			where = where + " doc#>>$" + strconv.Itoa(argc) + string(i.Op) + "$" + strconv.Itoa(argc+1)
			args = append(args, parserKeyToPG(i.Left.Literal))
			args = append(args, i.Right.Literal)
			argc += 2
		case []fexpr.ExprGroup:
			where = where + " ("
			where, args, argc = createSqlWhereClauseLoop(i, where, args, argc)
			where = where + " )"
		}
		if len(eg) > 1 && i < len(eg)-1 {
			where = where + " " + parserJoinToPG(e.Join)
		}
	}
	return where, args, argc
}

func parserKeyToPG(key string) string {
	return "{" + strings.ReplaceAll(key, ".", ",") + "}"
}

func parserJoinToPG(joinOp fexpr.JoinOp) string {
	if joinOp == "&&" {
		return "AND"
	} else if joinOp == "||" {
		return "OR"
	} else {
		panic(fmt.Sprintf("error: invalid joinop: %s", joinOp))
	}
}

