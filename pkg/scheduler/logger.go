package scheduler

import (
	"fmt"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

type jobLogger struct {
	logger *zap.Logger
}

func (j jobLogger) toFields(keysAndValues ...interface{}) []zap.Field {
	fields := make([]zap.Field, 0)
	key := ""
	c := 0
	for _, v := range keysAndValues {
		if c == 0 {
			key = fmt.Sprintf("%s", v)
		} else if c == 1 {
			fields = append(fields, zap.Any(key, v))
			key = ""
			c = 0
		}
		c++
	}
	if key != "" {
		fields = append(fields, zap.String(key, ""))
	}

	return fields
}

func (j jobLogger) Info(msg string, keysAndValues ...interface{}) {
	j.logger.Debug(msg, j.toFields(keysAndValues)...)
}

func (j jobLogger) Error(err error, msg string, keysAndValues ...interface{}) {
	fields := make([]zap.Field, 1)
	fields[0] = zap.Error(err)
	fields = append(fields, j.toFields(keysAndValues)...)
	j.logger.Warn(msg, fields...)
}

var _ cron.Logger = (*jobLogger)(nil)
