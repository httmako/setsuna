package main

import (
	"bytes"
	"encoding/json"
	"sync"
	// "fmt"
	"github.com/nxadm/tail"
	"sigs.k8s.io/yaml"
	// "io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	// "strings"
	"github.com/dop251/goja"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
	// "runtime/pprof"
	// "github.com/httmako/jote"
)

type Config struct {
	Debug           bool
	Target          string
	MaxBatch        int
	MaxDelay        int
	ProgressFile    string
	JSTransformer   string
	JSMessageParser string
	Input           []Input
	ScanFrequency   int
}

type Input struct {
	Group   string
	Pattern string
}

type Log struct {
	Ts   string `json:"ts"`
	Host string `json:"host"`
	Msg  string `json:"msg"`
	Src  string `json:"src"`
}

func getEnv(name string, def string) string {
	val := os.Getenv(name)
	if val == "" {
		return def
	}
	return val
}

func num(value string) int {
	num, err := strconv.Atoi(value)
	if err != nil {
		panic(err)
	}
	return num
}

var lastSendTime time.Time
func Sending(logger *slog.Logger, logs []Log, target string) {
	j, err := json.Marshal(logs)
	if err != nil {
		logger.Error("error during marshal", "err", err)
		return
	}
	for {
        logger.Debug("Time since last send","d",time.Since(lastSendTime))
        lastSendTime = time.Now()
		logger.Info("Posting", "lines", len(logs), "bytes", len(j))
		res, err := http.Post(target, "application/json", bytes.NewReader(j))
		if err != nil || res.StatusCode > 299 {
			logger.Error("error during http post, retrying in 5s", "err", err)
			time.Sleep(5 * time.Second)
		} else {
			return
		}
	}
}

func Logging(logger *slog.Logger, logCh <-chan Log, flushCh <-chan bool, cfg Config) {
	logs := make([]Log, 0, cfg.MaxBatch)
	for {
		select {
		case log := <-logCh:
			logs = append(logs, log)
			if len(logs) >= cfg.MaxBatch {
				Sending(logger, logs, cfg.Target)
				SaveProgress(logger, cfg)
				logs = make([]Log, 0, cfg.MaxBatch)
			}
		case <-flushCh:
			if len(logs) > 0 {
				logger.Debug("flushing fr after flushCh", "len", len(logs))
				Sending(logger, logs, cfg.Target)
				SaveProgress(logger, cfg)
				logs = make([]Log, 0, cfg.MaxBatch)
			}
		}
	}
}

func Flushing(flushCh chan<- bool, sleepTime int) {
	for {
		time.Sleep(time.Duration(sleepTime) * time.Second)
		flushCh <- true
	}
}

func SaveProgress(logger *slog.Logger, cfg Config) {
	prog := map[string]int64{}
	var err error
	var offset int64
	tailsMutex.Lock()
	for _, t := range tails {
		offset, err = t.Tell()
		if err != nil {
			logger.Error("error during tail.Tell()", "err", err)
			tailsMutex.Unlock()
			return
		}
		prog[t.Filename] = offset
	}
	tailsMutex.Unlock()
	j, err := json.Marshal(prog)
	if err != nil {
		logger.Error("error during marshal", "err", err)
		return
	}
	err = os.WriteFile(cfg.ProgressFile, j, 0700)
	if err != nil {
		logger.Error("error writing progress file", "err", err)
	}
}

var tails map[string]*tail.Tail

func main() {
	tails = map[string]*tail.Tail{}
	// Config
	CONFIGLOCATION := os.Getenv("CONFIG")
	if CONFIGLOCATION == "" {
		CONFIGLOCATION = "./config.yaml"
	}
	cfg := Config{}
	configfile, err := os.ReadFile(CONFIGLOCATION)
	if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(configfile, &cfg)
	if err != nil {
		panic(err)
	}

	// Logger
	loglvl := new(slog.LevelVar)
	if cfg.Debug {
		loglvl.Set(slog.LevelDebug)
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: loglvl}))

	logCh := make(chan Log, cfg.MaxBatch)
	flushCh := make(chan bool)

	logger.Info("starting up", "target", cfg.Target, "files", cfg.Input, "maxbatch", cfg.MaxBatch, "maxdelay", cfg.MaxDelay)

	LoopInputAndTailFiles(cfg, logger, logCh, true)
	// Load new files as they come in
	go func() {
		for {
			time.Sleep(time.Duration(cfg.ScanFrequency) * time.Second)
			logger.Debug("Cleanup and load new files")
			CleanupAndCloseFiles(cfg, logger)
			LoopInputAndTailFiles(cfg, logger, logCh, false)
		}
	}()

	go Logging(logger, logCh, flushCh, cfg)
	go Flushing(flushCh, cfg.MaxDelay)

	// Wait for signal to exit
	idleConnsClosed := make(chan struct{})
	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt)
		signal.Notify(sigint, syscall.SIGINT)
		signal.Notify(sigint, syscall.SIGTERM)
		<-sigint
		logger.Info("Signal received, shutting down...")
		close(idleConnsClosed)
	}()

	<-idleConnsClosed
	logger.Info("Exiting")
}

func CleanupAndCloseFiles(cfg Config, logger *slog.Logger) {
	tailsMutex.Lock()
	for file, tail := range tails {
		if _, err := os.Stat(file); err != nil {
			logger.Debug("Removing tail", "file", file, "err", err)
			tail.Stop()
			tail.Cleanup()
			delete(tails, file)
		}
	}
	tailsMutex.Unlock()
}

func LoopInputAndTailFiles(cfg Config, logger *slog.Logger, logCh chan<- Log, useProgFile bool) {
	//Parse progress.json
	prog := map[string]int64{}
	if useProgFile {
		if _, err := os.Stat(cfg.ProgressFile); err == nil {
			logger.Info("progress file found, using it")
			pg, err := os.ReadFile(cfg.ProgressFile)
			if err != nil {
				panic(err)
			}
			err = json.Unmarshal(pg, &prog)
			if err != nil {
				panic(err)
			}
		}
	}
	host, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	for _, input := range cfg.Input {
		files, err := filepath.Glob(input.Pattern)
		if err != nil {
			panic(err)
		}
		if len(files) < 1 {
			logger.Error("no files matched with pattern", "pattern", input.Pattern)
			continue
		}
		logger.Debug("Tailing files matched by pattern", "pattern", input.Pattern)
		for _, file := range files {
			if _, ok := GetTailFromFilePath(file); ok {
				continue
			}
			logger.Debug("Tailing new file", "path", file)
			go func() {
				tconf := tail.Config{Follow: true, ReOpen: true}
				if val, ok := prog[file]; ok {
					logger.Debug("Found progress on file", "file", file, "prog", val)
					tconf.Location = &tail.SeekInfo{Offset: val}
				}
				t, err := tail.TailFile(file, tconf)
				if err != nil {
					logger.Error("error tailing file", "err", err)
					return
				}
				SetTail(file, t)

				transformer := CreateTransformer(cfg)

				for line := range t.Lines {
					//Create Log
					l := Log{
						Ts:   line.Time.Format("2006-01-02 15:04:05.999"),
						Host: host,
						Msg:  line.Text,
						Src:  line.Text,
					}
					//Fix log line not being JSON
					j := map[string]interface{}{}
					err := json.Unmarshal([]byte(line.Text), &j)
					if err != nil {
						j = map[string]interface{}{
							"msg": line.Text,
						}
					}
					j["_meta"] = map[string]interface{}{
						"file":   file,
						"group":  input.Group,
						"length": len(line.Text),
					}

					newSrc, err := json.Marshal(j)
					if err != nil {
						logger.Error("error: marshal meta-filled json failed", "err", err)
						continue
					}
					l.Src = string(newSrc)
					l.Msg = transformer.ParseMessage(l.Src)
					l.Src = transformer.TransformSource(l.Src)

					logCh <- l
				}
			}()
		}
	}
}

var tailsMutex sync.Mutex

// func LoopThroughTails(loopFunc func(file string, tail *tail.Tail)) {}
func GetTailFromFilePath(file string) (*tail.Tail, bool) {
	tailsMutex.Lock()
	tail, ok := tails[file]
	tailsMutex.Unlock()
	return tail, ok
}
func SetTail(file string, tail *tail.Tail) {
	tailsMutex.Lock()
	tails[file] = tail
	tailsMutex.Unlock()
}

type Transformer struct {
	VM                *goja.Runtime
	MessageParser     goja.Callable
	SourceTransformer goja.Callable
}

func CreateTransformer(cfg Config) Transformer {
	vm := goja.New()
	_, err := vm.RunString(cfg.JSTransformer)
	if err != nil {
		panic(err)
	}
	_, err = vm.RunString(cfg.JSMessageParser)
	if err != nil {
		panic(err)
	}
	transformFunc, ok := goja.AssertFunction(vm.Get("t"))
	if !ok {
		panic("t is not a function")
	}
	messageFunc, ok := goja.AssertFunction(vm.Get("m"))
	if !ok {
		panic("m is not a function")
	}
	return Transformer{
		VM:                vm,
		MessageParser:     messageFunc,
		SourceTransformer: transformFunc,
	}
}

func (t *Transformer) TransformSource(line string) string {
	result, err := t.SourceTransformer(goja.Undefined(), t.VM.ToValue(line))
	if err != nil {
		panic(err)
	}
	return result.String()
}

func (t *Transformer) ParseMessage(line string) string {
	result, err := t.MessageParser(goja.Undefined(), t.VM.ToValue(line))
	if err != nil {
		panic(err)
	}
	return result.String()
}
