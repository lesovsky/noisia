package log

import (
	"fmt"
	"github.com/rs/zerolog"
	"os"
)

// Logger is the global logger with predefined settings
var Logger = zerolog.New(os.Stdout).With().Timestamp().Logger()

// KV is a simple key-value store
type KV map[string]string

// SetLevel sets logging level
func SetLevel(level string) {
	switch level {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
}

func New() zerolog.Logger {
	var logger = Logger
	return logger
}

// SetApplication appends application name string to log messages
func SetApplication(app string) {
	Logger = Logger.With().Str("service", app).Logger()
}

// Debug prints message with DEBUG severity
func Debug(msg string) {
	Logger.Debug().Msg(msg)
}

// Debugf prints formatted message with DEBUG severity
func Debugf(format string, v ...interface{}) {
	Logger.Debug().Msgf(format, v...)
}

// Debugln concatenates arguments and prints them with DEBUG severity
func Debugln(v ...interface{}) {
	Logger.Debug().Msg(fmt.Sprint(v...))
}

// Info prints message with INFO severity
func Info(msg string) {
	Logger.Info().Msg(msg)
}

// Infof prints formatted message with INFO severity
func Infof(format string, v ...interface{}) {
	Logger.Info().Msgf(format, v...)
}

// Infoln concatenates arguments and prints them with INFO severity
func Infoln(v ...interface{}) {
	Logger.Info().Msg(fmt.Sprint(v...))
}

// Warn prints message with WARNING severity
func Warn(msg string) {
	Logger.Warn().Msg(msg)
}

// Warnf prints formatted message with WARNING severity
func Warnf(format string, v ...interface{}) {
	Logger.Warn().Msgf(format, v...)
}

// Warnln concatenates arguments and prints them with WARNING severity
func Warnln(v ...interface{}) {
	Logger.Warn().Msg(fmt.Sprint(v...))
}

// Error prints message with ERROR severity
func Error(msg string) {
	Logger.Error().Msg(msg)
}

// Errorf prints formatted message with ERROR severity
func Errorf(format string, v ...interface{}) {
	Logger.Error().Msgf(format, v...)
}

// Errorln concatenates arguments and prints them with ERROR severity
func Errorln(v ...interface{}) {
	Logger.Error().Msg(fmt.Sprint(v...))
}

// KVError prints message with ERROR severity with attached KV map
func KVError(kv KV, msg string) {
	log := Logger.Error()
	for k, v := range kv {
		log.Str(k, v)
	}
	log.Msg(msg)
}

// KVErrorf prints formatted message with ERROR severity with attached KV map
func KVErrorf(kv KV, format string, v ...interface{}) {
	log := Logger.Error()
	for k, v := range kv {
		log.Str(k, v)
	}
	log.Msgf(format, v...)
}

// KVErrorln concatenates arguments and prints them with ERROR severity with attached KV map
func KVErrorln(kv KV, v ...interface{}) {
	log := Logger.Error()
	for k, v := range kv {
		log.Str(k, v)
	}
	log.Msg(fmt.Sprint(v...))
}
