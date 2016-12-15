package invalidate

import (
	"fmt"
	"github.com/beevik/etree"
	awsauth "github.com/smartystreets/go-aws-auth"
	"io"
	"net/http"
	"time"
)

// Config is the configuration object for the invalidator
type Config struct {
	Distribution struct {
		Id        string
		Accesskey string
		Key       string
	}
	Invalidation []string
}

// Do sends the invalidation URLs to cloudfront
func Do(config *Config, dryRun bool, logger io.Writer) error {
	if len(config.Invalidation) == 0 {
		return fmt.Errorf("no invalidation paths specified")
	}
	if config.Distribution.Id == "" {
		return fmt.Errorf("no distribution specified")
	}
	fmt.Fprintf(logger, "Invalidating %d Cloudfront URLs\n", len(config.Invalidation))
	if dryRun {
		for _, path := range config.Invalidation {
			fmt.Fprintln(logger, path)
		}
	} else {
		doc := createXML(config)
		if err := invalidate(doc, config, logger); err != nil {
			return err
		}
	}
	return nil
}

// createXML creates the request payload for the invalidation request
func createXML(config *Config) *etree.Document {
	doc := etree.NewDocument()
	doc.CreateProcInst("xml", `version="1.0" encoding="UTF-8"`)
	invalidationBatch := doc.CreateElement("InvalidationBatch")
	callerReference := invalidationBatch.CreateElement("CallerReference")
	callerReference.SetText(fmt.Sprintf("%s - %s", config.Distribution.Id, time.Now()))
	paths := invalidationBatch.CreateElement("Paths")
	items := paths.CreateElement("Items")
	for _, path := range config.Invalidation {
		item := items.CreateElement("Path")
		item.SetText(path)
	}
	quantity := paths.CreateElement("Quantity")
	quantity.SetText(fmt.Sprintf("%d", len(config.Invalidation)))
	return doc
}

// invalidate executes the invalidation request
func invalidate(doc *etree.Document, config *Config, logger io.Writer) error {
	errors := make(chan error, 1)
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		if _, err := doc.WriteTo(pw); err != nil {
			errors <- fmt.Errorf("could not write xml, %v", err)
		}
		errors <- nil
	}()

	client := &http.Client{}
	req, err := http.NewRequest("POST", fmt.Sprintf("https://cloudfront.amazonaws.com/2016-11-25/distribution/%s/invalidation", config.Distribution.Id), pr)
	if err != nil {
		return fmt.Errorf("could not invalidate paths, %v", err)
	}
	awsauth.Sign(req, awsauth.Credentials{
		AccessKeyID:     config.Distribution.Accesskey,
		SecretAccessKey: config.Distribution.Key,
	})
	resp, err := client.Do(req)
	defer resp.Body.Close()
	_, err = io.Copy(logger, resp.Body)
	if err != nil {
		return fmt.Errorf("could not read response from aws, %v", err)
	}
	if err := <-errors; err != nil {
		return err
	}
	return nil
}
