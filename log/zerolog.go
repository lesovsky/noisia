package log

import (
	"github.com/rs/zerolog"
	"os"
	"time"
)

// defaultLogger implements Logger interface using zerolog.Logger.
type defaultLogger struct {
	logger zerolog.Logger
}

// NewDefaultLogger creates new default logger.
func NewDefaultLogger() Logger {
	l := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).Level(zerolog.InfoLevel).With().Timestamp().Logger()

	return &defaultLogger{logger: l}
}

func (l *defaultLogger) Info(msg string) {
	l.logger.Info().Msg(msg)
}

func (l *defaultLogger) Infof(format string, v ...interface{}) {
	l.logger.Info().Msgf(format, v...)
}

func (l *defaultLogger) Warn(msg string) {
	l.logger.Warn().Msg(msg)
}

func (l *defaultLogger) Warnf(format string, v ...interface{}) {
	l.logger.Warn().Msgf(format, v...)
}

func (l *defaultLogger) Error(msg string) {
	l.logger.Error().Msg(msg)
}

func (l *defaultLogger) Errorf(format string, v ...interface{}) {
	l.logger.Error().Msgf(format, v...)
}
