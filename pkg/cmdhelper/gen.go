package cmdhelper

import (
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

type Flags []Flag

type Flag struct {
	// e.g. foo
	Name string
	// With struct hierarchy prefix
	// e.g. prefix-foo
	FullName string

	// Enable env
	EnableEnv bool
	// With struct hierarchy prefix
	// e.g. PREFIX_FOO
	FullEnv string

	Split     string
	Shorthand string
	Usage     string

	Type    string
	Value   interface{}
	Pointer interface{}
}

func resolveFieldName(field reflect.StructField) string {
	if name := field.Tag.Get("name"); name != "" {
		return name
	}

	// Use field name
	return toSnake(field.Name)
}

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func toSnake(name string) string {
	snake := matchFirstCap.ReplaceAllString(name, "${1}-${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}-${2}")
	return strings.ToLower(snake)
}

func resolveFlags(obj interface{}, flags Flags, namePrefix string) Flags {
	t := reflect.TypeOf(obj)
	if t.Kind() == reflect.Ptr && t.Elem().Kind() == reflect.Struct {
		t = t.Elem()
	}

	v := reflect.ValueOf(obj)
	if v.Kind() == reflect.Ptr && v.Elem().Kind() == reflect.Struct {
		v = v.Elem()
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.Type.Kind() == reflect.Struct {
			name := resolveFieldName(field)
			flags = resolveFlags(v.Field(i).Addr().Interface(), flags, genFullName(namePrefix, name))
		}

		tag := field.Tag
		name := resolveFieldName(field)
		env := tag.Get("enable-env")
		flagType := tag.Get("type")

		if name != "" && flagType != "" {
			flag := Flag{
				Name:      name,
				FullName:  genFullName(namePrefix, name),
				Split:     tag.Get("split"),
				Shorthand: tag.Get("shorthand"),
				Usage:     tag.Get("usage"),
				Type:      flagType,
				Value:     v.Field(i).Interface(),
				Pointer:   v.Field(i).Addr().Interface(),
			}

			// Enable env
			enableEnv, _ := strconv.ParseBool(env)
			if enableEnv {
				flag.EnableEnv = true
				flag.FullEnv = genEnv(flag.FullName)
			}

			flags = append(flags, flag)
		}
	}
	return flags
}

func genFullName(namePrefix, name string) string {
	var fullName string
	if namePrefix != "" {
		fullName = namePrefix + "-" + name
	} else {
		fullName = name
	}
	return fullName
}

// genEnv replace "-" to "_", and upper all character
func genEnv(name string) string {
	return strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
}

// ResolveFlagVariable register flags and env
func ResolveFlagVariable(cmd *cobra.Command, f interface{}) {
	t := reflect.TypeOf(f)
	if t.Kind() != reflect.Ptr {
		panic("flag variable require pointer type")
	}

	var flags Flags
	flags = resolveFlags(f, flags, "")

	// Check full name conflict
	set := make(map[string]struct{})
	for _, v := range flags {
		if _, ok := set[v.FullName]; ok {
			panic(fmt.Sprintf("flag full name conflict, %s", v.FullName))
		} else {
			set[v.FullName] = struct{}{}
		}
	}

	// Register flags to cobra
	for _, v := range flags {
		switch v.Type {
		case "bool":
			cmd.PersistentFlags().BoolVarP(v.Pointer.(*bool), v.FullName, v.Shorthand, v.Value.(bool), v.Usage)
		case "string":
			cmd.PersistentFlags().StringVarP(v.Pointer.(*string), v.FullName, v.Shorthand, v.Value.(string), v.Usage)
		case "int":
			cmd.PersistentFlags().IntVarP(v.Pointer.(*int), v.FullName, v.Shorthand, v.Value.(int), v.Usage)
		case "string-slice":
			cmd.PersistentFlags().StringSliceVarP(v.Pointer.(*[]string), v.FullName, v.Shorthand, v.Value.([]string), v.Usage)
		case "int-slice":
			cmd.PersistentFlags().IntSliceVarP(v.Pointer.(*[]int), v.FullName, v.Shorthand, v.Value.([]int), v.Usage)
		case "string-to-string":
			cmd.PersistentFlags().StringToStringVarP(v.Pointer.(*map[string]string), v.FullName, v.Shorthand, v.Value.(map[string]string), v.Usage)
		default:
			panic(fmt.Sprintf("not supported flag type: %s", v.Type))
		}
	}

	// Register env
	for _, v := range flags {
		if !v.EnableEnv {
			continue
		}

		flag := cmd.Flag(v.FullName)
		if flag == nil {
			continue
		}

		if flag.Usage == "" {
			flag.Usage = fmt.Sprintf("[env %v]", v.FullEnv)
		} else {
			flag.Usage = fmt.Sprintf("%v [env %v]", flag.Usage, v.FullEnv)
		}
		if value := os.Getenv(v.FullEnv); value != "" {
			// stringArray 和 stringSlice 可以同时从 env 和 args 里添加
			if v.Split != "" {
				strings.Split(value, v.Split)

				for _, v := range strings.Split(value, v.Split) {
					flag.Value.Set(v)
				}
			} else {
				flag.Value.Set(value)
			}
		}
	}
}
