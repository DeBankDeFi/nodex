// Copyright (c) 2022 DeBank Inc. <admin@debank.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.Logger

func init() {
	ProductionModeWithoutStackTrace()
}

func DevelopmentMode() {
	config := zap.NewDevelopmentConfig()

	buildLoggerWithConfig(config)
}

func DevelopmentModeWithoutStackTrace() {
	config := zap.NewDevelopmentConfig()
	config.DisableStacktrace = true

	buildLoggerWithConfig(config)
}

func ProductionMode() {
	config := zap.NewProductionConfig()
	config.EncoderConfig.EncodeCaller = zapcore.ShortCallerEncoder

	buildLoggerWithConfig(config)
}

func ProductionModeWithoutStackTrace() {
	config := zap.NewProductionConfig()
	config.EncoderConfig.EncodeCaller = zapcore.ShortCallerEncoder
	config.DisableStacktrace = true

	buildLoggerWithConfig(config)
}

func IntegrationTestMod() {
	config := zap.NewProductionConfig()
	config.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	config.EncoderConfig.EncodeCaller = zapcore.ShortCallerEncoder

	buildLoggerWithConfig(config)
}

func buildLoggerWithConfig(config zap.Config) {
	zapLogger, err := config.Build(zap.AddCallerSkip(1))
	if err != nil {
		panic("init zap logger: " + err.Error())
	}
	logger = zapLogger
}

func Any(key string, value interface{}) zap.Field {
	return zap.Any(key, value)
}

// Logger ...
func Logger() *zap.Logger {
	return logger.WithOptions(zap.AddCallerSkip(-1))
}

// Info ...
func Info(msg string, fields ...zap.Field) {
	logger.Info(msg, fields...)
}

// Warn ...
func Warn(msg string, fields ...zap.Field) {
	logger.Warn(msg, fields...)
}

// Error ...
func Error(msg string, err error, fields ...zap.Field) {
	fields = append(fields, zap.Error(err))
	logger.Error(msg, fields...)
}

// Debug  ...
func Debug(msg string, fields ...zap.Field) {
	logger.Debug(msg, fields...)
}

// Fatal ...
func Fatal(msg string, err error, fields ...zap.Field) {
	fields = append(fields, zap.Error(err))
	logger.Fatal(msg, fields...)
}
