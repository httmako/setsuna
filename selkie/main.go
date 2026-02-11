package main

import (
	"database/sql"
	"log/slog"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"sigs.k8s.io/yaml"
)

type Config struct {
	SQLConnectionString string `json:"sqlconnectionstring"`
	RunAsDaemon         bool
	DaemonRunInterval   int
	EnableMaxAgeCleaning bool
	MaxAgeForAll        string
	Cleanup             []Cleanup
}
type Cleanup struct {
	Key     string
	Value   string
	KeepFor string
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	st := time.Now()
	logger.Info("startup")

	CONFIGLOCATION := os.Getenv("CONFIG")
	if CONFIGLOCATION == "" {
		CONFIGLOCATION = "./config.yaml"
	}
	config := Config{}
	configfile, err := os.ReadFile(CONFIGLOCATION)
	if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(configfile, &config)
	if err != nil {
		panic(err)
	}

	db, err := sql.Open("postgres", config.SQLConnectionString)
	if err != nil {
		panic(err)
	}
	if err := db.Ping(); err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(1)
	// go func(){ for { fmt.Println(db.Stats()) time.Sleep(3*time.Second) } }()

	for {
		for _, cleanConfig := range config.Cleanup {
			logger.Info("cleaning", "key", cleanConfig.Key, "value", cleanConfig.Value, "keepfor", cleanConfig.KeepFor)
			key := "{" + strings.ReplaceAll(cleanConfig.Key, ".", ",") + "}"
			res, err := db.Exec("DELETE FROM docs WHERE doc#>>$1 = $2 AND ts < CURRENT_TIMESTAMP - INTERVAL '"+cleanConfig.KeepFor+"'", key, cleanConfig.Value)
			if err != nil {
				logger.Error("error during cleanup db.Exec", "err", err)
				continue
			}
			count, err := res.RowsAffected()
			logger.Info("cleaned", "count", count, "err", err)
		}
		if config.EnableMaxAgeCleaning {
			logger.Info("cleaningAll", "MaxAgeForAll", config.MaxAgeForAll)
			res, err := db.Exec("DELETE FROM docs WHERE ts < CURRENT_TIMESTAMP - INTERVAL '" + config.MaxAgeForAll + "'")
			if err != nil {
				logger.Error("error during cleanup db.Exec", "err", err)
			} else {
				count, err := res.RowsAffected()
				logger.Info("cleaned", "count", count, "err", err)
			}
		} else {
			logger.Info("config MaxAgeForAll missing, skipping")
		}
		if !config.RunAsDaemon {
			break
		}
		logger.Info("sleeping for", "minutes", config.DaemonRunInterval)
		time.Sleep(time.Duration(config.DaemonRunInterval) * time.Minute)

	}
	logger.Info("shutdown", "timetaken", time.Since(st))
}
