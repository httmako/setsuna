package main

import (
	"database/sql"
	"encoding/json/v2"
	"strings"
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
	Port                int       `json:"port"`
	SQLConnectionString string    `json:"sqlconnectionstring"`
	SQLMaxConnections   int       `json:"sqlmaxconnections"`
	CleanupInterval     int       `json:"cleanupinterval"`
	CleanupMaxAgeAll    string    `json:"cleanupmaxageall"`
	CleanupConfig       []Cleanup `json:"cleanupconfig"`
}

type Cleanup struct {
	Key     string
	Value   string
	KeepFor string
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

	go DoCleanupForever(config)

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

func DoCleanupForever(config Config) {
	for {
		logger.Info("sleeping until cleanup interval", "hours", config.CleanupInterval)
		time.Sleep(time.Duration(config.CleanupInterval) * time.Hour)
		logger.Info("starting cleanup")
		for _, cleanConfig := range config.CleanupConfig {
			if cleanConfig.Key == "" || cleanConfig.Value == "" || cleanConfig.KeepFor == "" {
				logger.Warn("invalid cleanup config, empty key/value/keepfor", "key", cleanConfig.Key, "value", cleanConfig.Value, "keepfor", cleanConfig.KeepFor)
				continue
			}
			logger.Info("cleaning with config", "key", cleanConfig.Key, "value", cleanConfig.Value, "keepfor", cleanConfig.KeepFor)
			key := "{" + strings.ReplaceAll(cleanConfig.Key, ".", ",") + "}"
			res, err := db.Exec("DELETE FROM docs WHERE doc#>>$1 = $2 AND ts < CURRENT_TIMESTAMP - INTERVAL '"+cleanConfig.KeepFor+"'", key, cleanConfig.Value)
			if err != nil {
				logger.Error("error during cleanup db.Exec", "err", err)
				continue
			}
			count, err := res.RowsAffected()
			logger.Info("cleaned", "key", cleanConfig.Key, "value", cleanConfig.Value, "keepfor", cleanConfig.KeepFor, "count", count, "err", err)
		}
		if config.CleanupMaxAgeAll != "" {
			logger.Info("cleaning all logs", "CleanupMaxAgeAll", config.CleanupMaxAgeAll)
			res, err := db.Exec("DELETE FROM docs WHERE ts < CURRENT_TIMESTAMP - INTERVAL '" + config.CleanupMaxAgeAll + "'")
			if err != nil {
				logger.Error("error during cleanup db.Exec", "err", err)
			} else {
				count, err := res.RowsAffected()
				logger.Info("cleaned all logs", "count", count, "err", err)
			}
		} else {
			logger.Info("config CleanupMaxAgeAll missing, skipping")
		}
		logger.Info("cleanup finished")
	}
}
