package shared

import (
	"github.com/google/uuid"
)

func AppInfo() Info {
	return appInfo
}

func SetAppName(name string) {
	appInfo.AppName = name
}

// GetAppName
func GetAppName() string {
	return appInfo.AppName
}

// Generate application runtime id
func RuntimeID() string {
	return AppInfo().AppName + "." + uuid.New().String()
}
