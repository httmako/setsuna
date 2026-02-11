package main

import (
	"database/sql"
	"encoding/json/v2"
	// "fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/httmako/jote"
	"github.com/lib/pq"
)

type Log struct {
	Ts   string `json:"ts"`
	Host string `json:"host"`
	Msg  string `json:"msg"`
	Doc  any    `json:"doc"`
}

type Config struct {
	Port                int    `json:"port"`
	SQLConnectionString string `json:"sqlconnectionstring"`
	SQLMaxConnections   int    `json:"sqlmaxconnections"`
}

var db *sql.DB
var logger *slog.Logger

func main() {
	jote.ProfilingUntilTimeIfSet(30)
	logger = jote.CreateLogger("stdout")

	CONFIGLOCATION := os.Getenv("CONFIG")
	if CONFIGLOCATION == "" {
		CONFIGLOCATION = "./config.yaml"
	}
	config := Config{}
	jote.ReadConfigYAML(CONFIGLOCATION, &config)

	var err error
	db, err = sql.Open("postgres", config.SQLConnectionString)
	jote.Must(err)
	jote.Must(db.Ping())
	db.SetMaxOpenConns(config.SQLMaxConnections)
	// go func(){ for { fmt.Println(db.Stats()) time.Sleep(3*time.Second) } }()

	jote.Must2(db.Exec("CREATE TABLE IF NOT EXISTS docs(id BIGSERIAL, ts TIMESTAMP, doc jsonb)"))
	jote.Must2(db.Exec("CREATE INDEX IF NOT EXISTS id ON docs (id)"))
	jote.Must2(db.Exec("CREATE INDEX IF NOT EXISTS j ON docs USING GIN (doc)"))

	mux := http.NewServeMux()
	RequestCounter := atomic.Uint64{}
	jote.AddMetrics(mux, "setsuna", &RequestCounter)

	mux.HandleFunc("POST /v1/effie/logs", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil || len(body) < 1 {
			logger.Error("error reading body", "ip", jote.HttpRequestGetIP(r), "err", err)
			return
		}
		saveEffieLogs(r, body)
	})

	jote.RunMux(":"+strconv.Itoa(config.Port), jote.AddLoggingToMuxWithCounter(mux, logger, &RequestCounter), logger)
}

func saveEffieLogs(r *http.Request, input []byte) {
	js := []map[string]any{}
	jote.Must(json.Unmarshal(input, &js))
	tx, err := db.BeginTx(r.Context(), &sql.TxOptions{})
	jote.Must(err)
	defer func() {
		if r := recover(); r != nil {
			logger.Warn("Rolling back saveArray because of panic", "err", r)
			tx.Rollback()
		}
	}()
	stmt, err := tx.Prepare(pq.CopyIn("docs", "ts", "doc"))
	jote.Must(err)

	t := time.Now().Format("2006-01-02 15:04:05.999")
	for _, j := range js {
		jote.Must2(stmt.ExecContext(r.Context(),
			getKeyOrDefault(j, "ts", t),
			getKeyOrDefault(j, "doc", "<NO/DOC>"),
		))
	}
	jote.Must2(stmt.Exec())
	jote.Must(stmt.Close())
	jote.Must(tx.Commit())
}

func getKeyOrDefault(j map[string]any, key string, def string) string {
	if v, ok := j[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return def
}
