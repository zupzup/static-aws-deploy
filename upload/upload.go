package upload

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"github.com/beevik/etree"
	awsauth "github.com/smartystreets/go-aws-auth"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Header is a simple HTTP header representation
type Header map[string]string

// Files is a map from filename to a HTTP header configuration
type Files map[string][]Header

// Delta is a mapping of files to their DeltaProperties
type Delta map[string]*DeltaProperties

// DeltaProperties are the properties of a file used to determine if it has been changed
type DeltaProperties struct {
	LastModified time.Time
	ETag         string
}

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
// and the headers to set for those files
func ParseFiles(config *Config, delta bool) (Files, error) {
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
	deltaMap := make(Delta)
	if delta {
		deltaMap, err = getDeltaMap(config)
		if err != nil {
			return nil, err
		}
	}
	err = filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && !re.MatchString(path) {
			hasChanged := true
			if delta {
				hasChanged, err = hasFileChanged(info, deltaMap, getUploadPath(config, path), path)
				if err != nil {
					return err
				}
			}
			if hasChanged {
				result[path] = []Header{}
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("could not read source directory: %s, %v", source, err)
	}
	for file := range result {
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

// hasFileChanged checks the md5 sum against the ETag of the uploaded files as well as the
// lastmodified date
func hasFileChanged(info os.FileInfo, deltaMap Delta, uploadPath, filePath string) (bool, error) {
	etag, err := calculateETag(filePath)
	if err != nil {
		return false, err
	}
	deltaProps := deltaMap[uploadPath]
	if deltaProps != nil {
		return etag != deltaProps.ETag && info.ModTime().After(deltaProps.LastModified), nil
	}
	return true, nil
}

// calculateETag generates the md5 sum of the given file
func calculateETag(path string) (string, error) {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("could not read file: %s while calculating it's ETag, %v", path, err)
	}
	return fmt.Sprintf("%x", md5.Sum(bytes)), nil
}

// getDeltaMap fetches all files from S3 and returns their keys and ETags
func getDeltaMap(config *Config) (Delta, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", fmt.Sprintf("https://s3.amazonaws.com/%s/?list-type=2", config.Bucket.Name), nil)
	if err != nil {
		return nil, fmt.Errorf("could not get bucket for delta upload, %v", err)
	}
	awsauth.Sign(req, awsauth.Credentials{
		AccessKeyID:     config.Bucket.Accesskey,
		SecretAccessKey: config.Bucket.Key,
	})
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("could not execute request to aws, %v", err)
	}
	defer resp.Body.Close()
	doc := etree.NewDocument()
	if _, err := doc.ReadFrom(resp.Body); err != nil {
		return nil, fmt.Errorf("could not parse response from aws, %v", err)
	}
	root := doc.SelectElement("ListBucketResult")
	if root == nil {
		return nil, fmt.Errorf("could not parse response from aws, xml is malformed: missing ListBucketResult")
	}
	deltaMap := make(Delta)
	contents := root.SelectElements("Contents")
	if contents == nil {
		return nil, fmt.Errorf("could not parse response from aws, xml is malformed: missing Contents")
	}
	for _, file := range contents {
		lastModified := file.SelectElement("LastModified")
		etag := file.SelectElement("ETag")
		key := file.SelectElement("Key")
		if lastModified == nil || etag == nil || key == nil {
			return nil, fmt.Errorf("could not parse response from aws, xml is malformed: Contents is missing ETag, Key or LastModified")
		}
		parsedLastModified, err := time.Parse(time.RFC3339Nano, lastModified.Text())
		if err != nil {
			return nil, fmt.Errorf("could not parse date in response from aws: %s, %v", lastModified, err)
		}
		deltaProp := DeltaProperties{
			ETag:         strings.Trim(etag.Text(), "\""),
			LastModified: parsedLastModified,
		}
		deltaMap[key.Text()] = &deltaProp
	}
	return deltaMap, nil
}

// Do iterates over the files concurrently and calls
// uploadFile for each file, printing progress indication to logger
func Do(config *Config, files Files, dryRun, delta bool, logger io.Writer) error {
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
		finishMessage := "Upload finished."
		if dryRun {
			finishMessage = "Dry Run finished."
		}
		if delta {
			finishMessage = fmt.Sprintf("Delta %s", finishMessage)
		}
		fmt.Fprintln(logger, finishMessage)
	case err := <-errors:
		if err != nil {
			return err
		}
	}
	return nil
}

// getUploadPath strips the sourceFolder and cleans the path for uploading
func getUploadPath(config *Config, filePath string) string {
	return strings.TrimPrefix(filePath, fmt.Sprintf("%s/", filepath.Clean(config.Source)))
}

// uploadFile uploads a file to AWS S3 with the given headers,
// not chunked currently
func uploadFile(config *Config, file string, headers []Header, logger io.Writer) error {
	uploadPath := getUploadPath(config, file)
	f, err := os.Open(file)
	if err != nil {
		return fmt.Errorf("could not open file: %s, %v", file, err)
	}
	defer f.Close()

	fileContents, err := ioutil.ReadAll(f)
	if err != nil {
		return fmt.Errorf("could not read file: %s, %v", file, err)
	}
	client := &http.Client{}
	req, err := http.NewRequest("PUT", fmt.Sprintf("https://s3.amazonaws.com/%s/%s", config.Bucket.Name, uploadPath), bytes.NewBuffer(fileContents))
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
	if err != nil {
		return fmt.Errorf("could not execute request to aws, %v", err)
	}
	defer resp.Body.Close()
	_, err = io.Copy(logger, resp.Body)
	if err != nil {
		return fmt.Errorf("could not read response from aws, %v", err)
	}
	return nil
}
