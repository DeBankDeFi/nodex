package utils

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

// Logger returns the global logger.
func Logger() *zap.Logger {
	return logger.WithOptions(zap.AddCallerSkip(-1))
}
