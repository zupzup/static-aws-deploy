package upload

import (
	"bytes"
	"fmt"
	awsauth "github.com/smartystreets/go-aws-auth"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

// Header is a simple HTTP header representation
type Header map[string]string

// Files is a map from filename to a HTTP header configuration
type Files map[string][]Header

// Config is the configuration object for the uploader
type Config struct {
	Bucket struct {
		Name      string
		Accesskey string
		Key       string
	}
	Parallel int
	Source   string
	Ignore   string
	Metadata []struct {
		Regex   string
		Headers []Header
	}
}

// ParseFiles builds a metadata object based on the sourcefiles and
// the provided configuration, which indicates which files will get uploaded
// and the headers to specify for each
func ParseFiles(config *Config) (Files, error) {
	source := config.Source
	if source == "" {
		return nil, fmt.Errorf("no source specified")
	}
	if _, err := os.Stat(source); err != nil {
		return nil, fmt.Errorf("could not read source directory %s, %v", source, err)
	}
	result := Files{}
	re, err := regexp.Compile(config.Ignore)
	if err != nil {
		return nil, fmt.Errorf("could not parse regex: %s, %v", config.Ignore, err)
	}
	err = filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && !re.MatchString(path) {
			result[path] = []Header{}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("could not read source directory: %s, %v", source, err)
	}
	for file, _ := range result {
		for _, metadata := range config.Metadata {
			re, err = regexp.Compile(metadata.Regex)
			if err != nil {
				return nil, fmt.Errorf("could not parse regex: %s, %v", metadata.Regex, err)
			}
			if re.MatchString(file) {
				for _, header := range metadata.Headers {
					result[file] = append(result[file], header)
				}
			}
		}
	}
	return result, nil
}

// Do iterates over the files concurrently and calls
// uploadFile for each file, printing progress indication to logger
func Do(config *Config, files Files, dryRun bool, logger io.Writer) error {
	if config.Bucket.Name == "" {
		return fmt.Errorf("no bucket specified")
	}
	poolSize := config.Parallel
	wg := sync.WaitGroup{}
	pool := make(chan struct{}, poolSize)
	errors := make(chan error, 1)
	finished := make(chan bool, 1)

	fmt.Fprintf(logger, "%d Files to upload (%d concurrently)...\n", len(files), poolSize)
	for key, value := range files {
		wg.Add(1)
		go func(config *Config, file string, headers []Header) {
			defer wg.Done()
			pool <- struct{}{}
			defer func() { <-pool }()
			if !dryRun {
				if err := uploadFile(config, file, headers, logger); err != nil {
					errors <- err
				}
			}
			fmt.Fprintf(logger, "%s...Done.\n", file)
		}(config, key, value)
	}

	go func() {
		wg.Wait()
		close(finished)
	}()

	select {
	case <-finished:
		fmt.Fprintln(logger, "Upload finished.")
	case err := <-errors:
		if err != nil {
			return err
		}
	}
	return nil
}

// uploadFile uploads a file to AWS S3 with the given headers,
// not chunked currently
func uploadFile(config *Config, file string, headers []Header, logger io.Writer) error {
	uploadPath := strings.TrimPrefix(file, fmt.Sprintf("%s/", filepath.Clean(config.Source)))
	f, err := os.Open(file)
	if err != nil {
		return fmt.Errorf("could not open file: %s, %v", file, err)
	}
	defer f.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile(uploadPath, filepath.Base(file))
	if err != nil {
		return fmt.Errorf("could not create upload file: %s, %v", filepath.Base(file), err)
	}
	_, err = io.Copy(part, f)
	if err != nil {
		return fmt.Errorf("could not create upload file: %s, %v", filepath.Base(file), err)
	}
	err = writer.Close()
	if err != nil {
		return fmt.Errorf("could not create upload file: %s, %v", filepath.Base(file), err)
	}

	client := &http.Client{}
	req, err := http.NewRequest("PUT", fmt.Sprintf("https://%s.s3.amazonaws.com/%s", config.Bucket.Name, uploadPath), body)
	if err != nil {
		return fmt.Errorf("could not upload file to bucket: %s, %v", config.Bucket.Name, err)
	}
	for _, header := range headers {
		for k, v := range header {
			req.Header.Add(k, v)
		}
	}
	awsauth.Sign(req, awsauth.Credentials{
		AccessKeyID:     config.Bucket.Accesskey,
		SecretAccessKey: config.Bucket.Key,
	})
	resp, err := client.Do(req)
	defer resp.Body.Close()
	_, err = io.Copy(logger, resp.Body)
	if err != nil {
		return fmt.Errorf("could not read response from aws, %v", err)
	}
	return nil
}
