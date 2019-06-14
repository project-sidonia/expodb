package loggingutils

import (
	"bytes"
	"io"

	"go.uber.org/zap"
)

func NewLogWriter(logger *zap.Logger) io.Writer {
	return &loggerWriter{logger}
}

type loggerWriter struct {
	logger *zap.Logger
}

func (l *loggerWriter) Write(p []byte) (int, error) {
	n := len(p)
	p = bytes.TrimSpace(p)
	l.logger.Info("", zap.ByteString("raw-msg", p))
	return n, nil
}
