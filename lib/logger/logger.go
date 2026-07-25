package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Settings stores config for Logger
type Settings struct {
	Path       string `yaml:"path"`
	Name       string `yaml:"name"`
	Ext        string `yaml:"ext"`
	TimeFormat string `yaml:"time-format"`
}

type LogLevel int

// Output levels
const (
	DEBUG LogLevel = iota
	INFO
	WARNING
	ERROR
	FATAL
)

const (
	flags              = log.LstdFlags
	defaultCallerDepth = 2
	bufferSize         = 1e5
)

type logEntry struct {
	msg   string
	level LogLevel
}

var (
	levelFlags = []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}

	minLevelMu sync.RWMutex
	minLevel   LogLevel = DEBUG
)

// SetMinLevel sets the minimum log level (messages below are dropped).
func SetMinLevel(level LogLevel) {
	minLevelMu.Lock()
	minLevel = level
	minLevelMu.Unlock()
}

// GetMinLevel returns the current minimum log level.
func GetMinLevel() LogLevel {
	minLevelMu.RLock()
	defer minLevelMu.RUnlock()
	return minLevel
}

// ParseRedisLogLevel maps Redis CONFIG loglevel names to LogLevel.
// debug/verbose → DEBUG, notice → INFO, warning → WARNING.
func ParseRedisLogLevel(s string) (LogLevel, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug", "verbose":
		return DEBUG, true
	case "notice", "info", "":
		return INFO, true
	case "warning", "warn":
		return WARNING, true
	default:
		return 0, false
	}
}

func shouldLog(level LogLevel) bool {
	minLevelMu.RLock()
	defer minLevelMu.RUnlock()
	return level >= minLevel
}

// ILogger defines the methods that any logger should implement
type ILogger interface {
	Output(level LogLevel, callerDepth int, msg string)
}

// Logger is Logger
type Logger struct {
	mu        sync.Mutex
	logFile   *os.File
	logger    *log.Logger
	entryChan chan *logEntry
	entryPool *sync.Pool
	// rotateSettings enables daily file rotation (godis default logs/).
	// When nil, output is fixed (stdout and/or Redis-style logfile).
	rotateSettings *Settings
}

var DefaultLogger ILogger = NewStdoutLogger()

// NewStdoutLogger creates a logger which print msg to stdout
func NewStdoutLogger() *Logger {
	logger := &Logger{
		logFile:   nil,
		logger:    log.New(os.Stdout, "", flags),
		entryChan: make(chan *logEntry, bufferSize),
		entryPool: &sync.Pool{
			New: func() interface{} {
				return &logEntry{}
			},
		},
	}
	go logger.writeLoop()
	return logger
}

// NewFileLogger creates a logger which print msg to stdout and log file
func NewFileLogger(settings *Settings) (*Logger, error) {
	fileName := fmt.Sprintf("%s-%s.%s",
		settings.Name,
		time.Now().Format(settings.TimeFormat),
		settings.Ext)
	logFile, err := mustOpen(fileName, settings.Path)
	if err != nil {
		return nil, fmt.Errorf("logging.Join err: %s", err)
	}
	mw := io.MultiWriter(os.Stdout, logFile)
	logger := &Logger{
		logFile:        logFile,
		logger:         log.New(mw, "", flags),
		entryChan:      make(chan *logEntry, bufferSize),
		rotateSettings: settings,
		entryPool: &sync.Pool{
			New: func() interface{} {
				return &logEntry{}
			},
		},
	}
	go logger.writeLoop()
	return logger, nil
}

func (logger *Logger) writeLoop() {
	for e := range logger.entryChan {
		logger.mu.Lock()
		if logger.rotateSettings != nil && logger.logFile != nil {
			settings := logger.rotateSettings
			logFilename := fmt.Sprintf("%s-%s.%s",
				settings.Name,
				time.Now().Format(settings.TimeFormat),
				settings.Ext)
			if path.Join(settings.Path, logFilename) != logger.logFile.Name() {
				logFile, err := mustOpen(logFilename, settings.Path)
				if err != nil {
					fmt.Fprintf(os.Stderr, "open log %s failed: %v\n", logFilename, err)
				} else {
					if logger.logFile != nil {
						_ = logger.logFile.Close()
					}
					logger.logFile = logFile
					logger.logger = log.New(io.MultiWriter(os.Stdout, logFile), "", flags)
				}
			}
		}
		l := logger.logger
		logger.mu.Unlock()
		_ = l.Output(0, e.msg)
		logger.entryPool.Put(e)
	}
}

// ReconfigureOutput switches DefaultLogger output.
// Empty logfile → stdout only; non-empty → append to that path (also mirrors stdout).
func ReconfigureOutput(logfile string) error {
	l, ok := DefaultLogger.(*Logger)
	if !ok || l == nil {
		DefaultLogger = NewStdoutLogger()
		l = DefaultLogger.(*Logger)
	}
	return l.reconfigureOutput(logfile)
}

func (logger *Logger) reconfigureOutput(logfile string) error {
	logger.mu.Lock()
	defer logger.mu.Unlock()

	var w io.Writer = os.Stdout
	var f *os.File
	if logfile != "" {
		dir := filepath.Dir(logfile)
		if dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0755); err != nil {
				return err
			}
		}
		var err error
		f, err = os.OpenFile(logfile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		w = io.MultiWriter(os.Stdout, f)
	}
	if logger.logFile != nil {
		_ = logger.logFile.Close()
		logger.logFile = nil
	}
	logger.logFile = f
	logger.logger = log.New(w, "", flags)
	logger.rotateSettings = nil // Redis logfile: fixed path, no daily rotate
	return nil
}

// Setup initializes DefaultLogger
func Setup(settings *Settings) error {
	logger, err := NewFileLogger(settings)
	if err != nil {
		// Fallback to stdout logger
		DefaultLogger = NewStdoutLogger()
		Errorf("create file logger failed: %v, fallback to stdout", err)
		return err
	}
	DefaultLogger = logger
	return nil
}

// Output sends a msg to logger
func (logger *Logger) Output(level LogLevel, callerDepth int, msg string) {
	if !shouldLog(level) {
		return
	}
	var formattedMsg string
	_, file, line, ok := runtime.Caller(callerDepth)
	if ok {
		formattedMsg = fmt.Sprintf("[%s][%s:%d] %s", levelFlags[level], filepath.Base(file), line, msg)
	} else {
		formattedMsg = fmt.Sprintf("[%s] %s", levelFlags[level], msg)
	}
	entry := logger.entryPool.Get().(*logEntry)
	entry.msg = formattedMsg
	entry.level = level
	logger.entryChan <- entry
}

// Debug logs debug message through DefaultLogger
func Debug(v ...interface{}) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(DEBUG, defaultCallerDepth, msg)
}

// Debugf logs debug message through DefaultLogger
func Debugf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	DefaultLogger.Output(DEBUG, defaultCallerDepth, msg)
}

// Info logs message through DefaultLogger
func Info(v ...interface{}) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(INFO, defaultCallerDepth, msg)
}

// Infof logs message through DefaultLogger
func Infof(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	DefaultLogger.Output(INFO, defaultCallerDepth, msg)
}

// Warn logs warning message through DefaultLogger
func Warn(v ...interface{}) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(WARNING, defaultCallerDepth, msg)
}

// Error logs error message through DefaultLogger
func Error(v ...interface{}) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(ERROR, defaultCallerDepth, msg)
}

// Errorf logs error message through DefaultLogger
func Errorf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	DefaultLogger.Output(ERROR, defaultCallerDepth, msg)
}

// Fatal prints error message then stop the program
func Fatal(v ...interface{}) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(FATAL, defaultCallerDepth, msg)
	fmt.Fprint(os.Stderr, msg)
	os.Exit(1)
}
