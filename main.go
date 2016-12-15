package main

import (
	"flag"
	"fmt"
	"github.com/zupzup/static-aws-deploy/invalidate"
	"github.com/zupzup/static-aws-deploy/upload"
	yaml "gopkg.in/yaml.v2"
	"io"
	"io/ioutil"
	"log"
	"os"
)

type Config struct {
	Auth struct {
		Accesskey string
		Key       string
	}
	S3         upload.Config
	Cloudfront invalidate.Config
}

var (
	configFile string
	dryRun     bool
	silent     bool
)

func init() {
	flag.StringVar(&configFile, "config", "./config.yml", "path to the configuration file (config.yml)")
	flag.StringVar(&configFile, "c", "./config.yml", "path to the configuration file (config.yml) (shorthand)")
	flag.BoolVar(&dryRun, "dry-run", false, "run the script without actually uploading or invalidating anything")
	flag.BoolVar(&dryRun, "dr", false, "run the script without actually uploading or invalidating anything (shorthand)")
	flag.BoolVar(&silent, "silent", false, "omit all log output")
	flag.BoolVar(&silent, "s", false, "omit all log output (shorthand)")
}

func main() {
	flag.Parse()
	config, err := readConfig(configFile)
	if err != nil {
		log.Fatal(err)
	}
	var logger io.Writer = os.Stdout
	if silent {
		logger = ioutil.Discard
	}
	files, err := upload.ParseFiles(&config.S3)
	if err != nil {
		log.Fatal(err)
	}
	if err := upload.Do(&config.S3, files, dryRun, logger); err != nil {
		log.Fatal(err)
	}
	if err := invalidate.Do(&config.Cloudfront, dryRun, logger); err != nil {
		log.Fatal(err)
	}
}

// readConfig reads the config from a given path and parses it
func readConfig(path string) (*Config, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("could not read config file: %v", err)
	}
	config := Config{}
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("could not parse config: %v", err)
	}
	if config.S3.Parallel <= 0 {
		config.S3.Parallel = 1
	}
	if config.Auth.Accesskey == "" {
		config.Auth.Accesskey = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	if config.Auth.Key == "" {
		config.Auth.Key = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
	if config.Auth.Key == "" || config.Auth.Accesskey == "" {
		return nil, fmt.Errorf("no aws credentials found")
	}
	config.S3.Bucket.Accesskey = config.Auth.Accesskey
	config.S3.Bucket.Key = config.Auth.Key
	config.Cloudfront.Distribution.Accesskey = config.Auth.Accesskey
	config.Cloudfront.Distribution.Key = config.Auth.Key
	return &config, nil
}
