package logger

import "context"

var log Logger

const (
	//Debug has verbose message
	Debug = "debug"
	//Info is default log level
	Info = "info"
	//Warn is for logging messages about possible issues
	Warn = "warn"
	//Error is for logging errors
	Error = "error"
	//Fatal is for logging fatal messages. The sytem shutsdown after logging the message.
	Fatal = "fatal"
)

type ctxKey string

const (
	withCtx ctxKey = "withCtx"
)

// Configuration stores the config for the logger
// For some loggers there can only be one level across writers, for such the level of Console is picked by default
type Configuration struct {
	EnableConsole     bool
	ConsoleJSONFormat bool
	ConsoleLevel      string
}

type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(err error, format string, args ...interface{})
	WithFields(keyValues map[string]interface{}) Logger
}

func Debugf(format string, args ...interface{}) {
	log.Debugf(format, args...)
}

func Infof(format string, args ...interface{}) {
	log.Infof(format, args...)
}

func Warnf(format string, args ...interface{}) {
	log.Warnf(format, args...)
}

func Errorf(format string, args ...interface{}) {
	log.Errorf(format, args...)
}

func Fatalf(err error, format string, args ...interface{}) {
	log.Fatalf(err, format, args...)
}

func WithFields(keyValues map[string]interface{}) Logger {
	return log.WithFields(keyValues)
}

func Context(ctx context.Context) Logger {
	if ctx != nil {
		if ctxRqID, ok := ctx.Value(withCtx).(string); ok {
			return log.WithFields(map[string]interface{}{
				"rqID": ctxRqID,
			})
		}
	}
	return log
}

func init() {
	var err error

	log, err = newZapLogger(Configuration{
		EnableConsole:     true,
		ConsoleLevel:      Debug,
		ConsoleJSONFormat: true,
	})

	if err != nil {
		panic(err)
	}
}
