package main

import (
	"compress/gzip"
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"log"
	"os"
	"strings"
	"sync"
)

const (
	region = endpoints.EuWest1RegionID
)

type FakeWriterAt struct {
	w io.Writer
}

func (fw FakeWriterAt) WriteAt(p []byte, offset int64) (n int, err error) {
	// ignore 'offset' because we forced sequential downloads
	return fw.w.Write(p)
}

func Gunzip(sourceBucket, destinationBucket, key string) {
	reader, writer := io.Pipe()
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)
	downloader := s3manager.NewDownloader(sess)

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer func() {
			wg.Done()
			writer.Close()
		}()
		numBytes, err := downloader.Download(FakeWriterAt{writer},
			&s3.GetObjectInput{
				Bucket: aws.String(sourceBucket),
				Key:    aws.String(key),
			})
		if err != nil {
			exitErrorf("Unable to download item %q, %v", key, err)
		}

		log.Printf("Downloaded %d bytes", numBytes)
	}()

	go func() {
		defer wg.Done()
		gzReader, _ := gzip.NewReader(reader)

		uploader := s3manager.NewUploader(sess)

		metadata := make(map[string]*string)
		metadata["Content-Type"] = aws.String("text/plain")

		result, err := uploader.Upload(&s3manager.UploadInput{
			Body:     gzReader,
			Bucket:   aws.String(destinationBucket),
			Key:      aws.String(strings.ReplaceAll(key, ".gz", "")),
			Metadata: metadata,
		})
		if err != nil {
			log.Fatalln("Failed to upload", err)
		}

		log.Println("Successfully uploaded to", result.Location)
	}()

	wg.Wait()
}

func HandleRequest(ctx context.Context, s3Event events.S3Event) {
	destinationBucket := os.Getenv("DESTINATION_BUCKET")
	for _, record := range s3Event.Records {
		s3obj := record.S3
		sourceBucket := s3obj.Bucket.Name
		key := s3obj.Object.Key
		Gunzip(sourceBucket, destinationBucket, key)
	}
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func main() {
	lambda.Start(HandleRequest)
}
