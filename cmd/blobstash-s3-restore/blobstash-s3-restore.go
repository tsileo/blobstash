package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"a4.io/blobstash/pkg/backend/s3/s3util"
	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/crypto"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func usage() {
	fmt.Printf("Usage: %s [OPTIONS] [CONFIG_FILE_PATH]\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 1 {
		usage()
		os.Exit(2)
	}
	var err error

	conf := &config.Config{}
	if flag.NArg() == 1 {
		conf, err = config.New(flag.Arg(0))
		if err != nil {
			log.Fatalf("failed to load config at \"%v\": %v", flag.Arg(0), err)
		}
	}
	packsDir := filepath.Join(conf.VarDir(), "blobs")
	os.MkdirAll(packsDir, 0700)
	region := conf.S3Repl.Region

	var sess *session.Session
	if conf.S3Repl.Endpoint != "" {
		sess, err = s3util.NewWithCustomEndoint(conf.S3Repl.AccessKey, conf.S3Repl.SecretKey, region, conf.S3Repl.Endpoint)
		if err != nil {
			panic(err)
		}
	} else {
		// Create a S3 Session
		sess, err = s3util.New(region)
		if err != nil {
			panic(err)
		}
	}

	key, err := conf.S3Repl.Key()
	if err != nil {
		panic(err)
	}

	downloader := s3manager.NewDownloader(sess)
	bucket := conf.S3Repl.Bucket
	s3svc := s3.New(sess)
	obucket := s3util.NewBucket(s3svc, bucket)
	packs, err := obucket.ListPrefix("packs/", "", 100)
	if err != nil {
		panic(err)
	}

	for _, pack := range packs {
		if err := BlobsFilesDownloadPack(key, downloader, packsDir, bucket, pack.Key); err != nil {
			panic(err)
		}
	}

	os.Exit(0)
}

func BlobsFilesDownloadPack(ckey *[32]byte, downloader *s3manager.Downloader, packDir, bucket, key string) error {
	f, err := ioutil.TempFile("", "blobstash_blobsfile_download")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err := downloader.Download(f, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	decrypted, err := crypto.Open(ckey, f.Name())
	if err != nil {
		return err
	}

	if err := os.Rename(decrypted, filepath.Join(packDir, filepath.Base(key))); err != nil {
		return err
	}

	return nil
}
