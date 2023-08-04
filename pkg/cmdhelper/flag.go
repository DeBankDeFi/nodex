package cmdhelper

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/spf13/cobra"
)

type EnvFlags map[string]EnvFlag

type EnvFlag struct {
	Env   string
	Split string
}

func updateEnvFlags(obj interface{}, envFlags EnvFlags) {
	if envFlags == nil {
		return
	}

	t := reflect.TypeOf(obj)
	v := reflect.ValueOf(obj)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.Type.Kind() == reflect.Struct {
			updateEnvFlags(v.Field(i).Interface(), envFlags)
		}

		tag := field.Tag
		name := tag.Get("name")
		env := tag.Get("env")
		split := tag.Get("split")

		if env != "" && name != "" {
			envFlags[name] = EnvFlag{
				Env:   env,
				Split: split,
			}
		}
	}
}

func ResolveEnvVariable(cmd *cobra.Command, f interface{}) {
	envFlags := make(map[string]EnvFlag)
	updateEnvFlags(f, envFlags)

	for flagName, env := range envFlags {
		flag := cmd.Flag(flagName)
		if flag == nil {
			continue
		}

		flag.Usage = fmt.Sprintf("%v [env %v]", flag.Usage, env.Env)
		if value := os.Getenv(env.Env); value != "" {
			if env.Split != "" {
				strings.Split(value, env.Split)

				for _, v := range strings.Split(value, env.Split) {
					flag.Value.Set(v)
				}
			} else {
				flag.Value.Set(value)
			}
		}
	}
}
