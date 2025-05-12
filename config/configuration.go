package config

import (
	"fmt"
	"github.com/spf13/viper"
	"reflect"
	"strconv"
	"strings"
)

type GigapiConfiguration struct {
	Enabled       bool    `json:"enabled" mapstructure:"enabled" default:"true"`
	Root          string  `json:"root" mapstructure:"root" default:""`
	MergeTimeoutS int     `json:"merge_timeout_s" mapstructure:"merge_timeout_s" default:"10"`
	Secret        string  `json:"secret" mapstructure:"secret" default:""`
	AllowSaveToHD bool    `json:"allow_save_to_hd" mapstructure:"allow_save_to_hd" default:"true"`
	SaveTimeoutS  float64 `json:"save_timeout_s" mapstructure:"save_timeout_s" default:"1"`
	NoMerges      bool    `json:"no_merges" mapstructure:"no_merges" default:"false"`
}

type BasicAuthConfiguration struct {
	Username string `json:"username" mapstructure:"username" default:""`
	Password string `json:"password" mapstructure:"password" default:""`
}

type Configuration struct {
	Gigapi    GigapiConfiguration    `json:"gigapi" mapstructure:"gigapi" default:""`
	Port      int                    `json:"port" mapstructure:"port" default:"7971"`
	Host      string                 `json:"host" mapstructure:"host" default:"0.0.0.0"`
	BasicAuth BasicAuthConfiguration `json:"basic_auth" mapstructure:"basic_auth" default:""`
}

var Config *Configuration

func InitConfig(file string) {
	viper.SetEnvPrefix("")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
	// If a file is provided, use it as the config file
	if file != "" {
		viper.SetConfigFile(file)
		err := viper.ReadInConfig()
		if err != nil {
			panic(fmt.Errorf("error reading config file: %s", err))
		}
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	} else {
		fmt.Println("Using environment variables for configuration")
	}

	Config = &Configuration{}
	err := viper.Unmarshal(Config)
	if err != nil {
		panic(fmt.Errorf("unable to decode into struct: %s", err))
	}
	if Config.Gigapi.SaveTimeoutS == 0 {
		Config.Gigapi.SaveTimeoutS = 1
	}
	setDefaults(Config)
	fmt.Printf("Loaded configuration: %+v\n", Config)
}

func setDefaults(config any) {
	configValue := reflect.ValueOf(config).Elem()
	configType := configValue.Type()

	for i := 0; i < configValue.NumField(); i++ {
		field := configValue.Field(i)
		fieldType := configType.Field(i)

		if field.Kind() == reflect.Struct {
			setDefaults(field.Addr().Interface())
			continue
		}

		defaultTag := fieldType.Tag.Get("default")
		if defaultTag == "" {
			continue
		}

		if field.IsZero() {
			switch field.Kind() {
			case reflect.String:
				field.SetString(defaultTag)
			case reflect.Int:
				if intValue, err := strconv.Atoi(defaultTag); err == nil {
					field.SetInt(int64(intValue))
				}
			case reflect.Float64:
				if floatValue, err := strconv.ParseFloat(defaultTag, 64); err == nil {
					field.SetFloat(floatValue)
				}
			case reflect.Bool:
				if boolValue, err := strconv.ParseBool(defaultTag); err == nil {
					field.SetBool(boolValue)
				}
			}
		}
	}
}
