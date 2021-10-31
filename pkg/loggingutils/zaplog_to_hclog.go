package loggingutils

import (
	"fmt"
	"io"
	"log"

	hclog "github.com/hashicorp/go-hclog"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewHclog2ZapLogger(z *zap.Logger) hclog.Logger {
	return hclog2ZapLogger{baseZap: z, zap: z}
}

// hclog2ZapLogger implements Hashicorp's hclog.Logger interface using Uber's zap.Logger. Wrapping it so we
// can use zap inside of Hashicorp's raft lib.
type hclog2ZapLogger struct {
	zap     *zap.Logger
	baseZap *zap.Logger
}

// Args are alternating key, val pairs
// keys must be strings
// vals can be any type, but display is implementation specific
// Emit a message and key/value pairs at a provided log level
func (l hclog2ZapLogger) Log(level hclog.Level, msg string, args ...interface{}) {
	switch level {
	case hclog.Debug:
		l.zap.Debug(msg, argsToFields(args...)...)
	case hclog.Trace:
	case hclog.Info:
		l.zap.Info(msg, argsToFields(args...)...)
	case hclog.Warn:
		l.zap.Warn(msg, argsToFields(args...)...)
	case hclog.Error:
		l.zap.Error(msg, argsToFields(args...)...)
	}

}

// Returns the Name of the logger
func (l hclog2ZapLogger) Name() string { return "zap-to-hclog-wrapper" }

// Trace implementation.
func (l hclog2ZapLogger) Trace(msg string, args ...interface{}) {}

// Debug implementation.
func (l hclog2ZapLogger) Debug(msg string, args ...interface{}) {
	l.zap.Debug(msg, argsToFields(args...)...)
}

// Info implementation.
func (l hclog2ZapLogger) Info(msg string, args ...interface{}) {
	l.zap.Info(msg, argsToFields(args...)...)
}

// Warn implementation.
func (l hclog2ZapLogger) Warn(msg string, args ...interface{}) {
	l.zap.Warn(msg, argsToFields(args...)...)
}

// Error implementation.
func (l hclog2ZapLogger) Error(msg string, args ...interface{}) {
	l.zap.Error(msg, argsToFields(args...)...)
}

// IsTrace implementation.
func (l hclog2ZapLogger) IsTrace() bool { return false }

// IsDebug implementation.
func (l hclog2ZapLogger) IsDebug() bool { return false }

// IsInfo implementation.
func (l hclog2ZapLogger) IsInfo() bool { return false }

// IsWarn implementation.
func (l hclog2ZapLogger) IsWarn() bool { return false }

// IsError implementation.
func (l hclog2ZapLogger) IsError() bool { return false }

// With implementation.
func (l hclog2ZapLogger) With(args ...interface{}) hclog.Logger {
	return hclog2ZapLogger{baseZap: l.zap, zap: l.zap.With(argsToFields(args...)...)}
}

// Named implementation.
func (l hclog2ZapLogger) Named(name string) hclog.Logger {
	return hclog2ZapLogger{baseZap: l.zap, zap: l.zap.Named(name)}
}

// ResetNamed implementation.
func (l hclog2ZapLogger) ResetNamed(name string) hclog.Logger {
	// no need to implement that as go-plugin doesn't use this method.
	return hclog2ZapLogger{baseZap: l.zap, zap: l.zap}
}

// ImpliedArgs returns With key/value pairs
func (l hclog2ZapLogger) ImpliedArgs() []interface{} {
	return nil
}

// SetLevel implementation.
func (l hclog2ZapLogger) SetLevel(level hclog.Level) {
}

// StandardLogger implementation.
func (l hclog2ZapLogger) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	return zap.NewStdLog(l.zap)
}

// StandardLogger implementation.
// Return a value that conforms to io.Writer, which can be passed into log.SetOutput()
func (l hclog2ZapLogger) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	return &loggerWriter{l.zap}
}

// Hclog has key-->values in the array as i=key i+1=value
func argsToFields(args ...interface{}) []zapcore.Field {
	fields := []zapcore.Field{}
	for i := 0; i < len(args); i += 2 {
		fields = append(fields, zap.String(args[i].(string), fmt.Sprintf("%v", args[i+1])))
	}

	return fields
}
