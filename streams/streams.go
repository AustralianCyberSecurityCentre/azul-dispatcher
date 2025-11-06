/*
Package streams allows for clients to upload and download binary stream data and their basic metadata.
It acts as the HTTP storage abstraction for the system.
*/
package streams

import (
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/streams/identify"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/streams/store"
)

type Streams struct {
	Store store.FileStorage
	// Identify
	identifier identify.Identifier
}

func (s *Streams) Close() {
	s.identifier.Close()
}

func NewStreams() *Streams {
	// create stream storage
	var fstore store.FileStorage
	var err error
	switch st.Streams.Backend {
	case "s3":
		// external s3 api
		if len(st.Streams.S3.AccessKey) == 0 && len(st.Streams.S3.SecretKey) == 0 {
			// Use credentials from service accounts by default
			fstore, err = store.NewS3StoreIAM(st.Streams.S3.Endpoint, st.Streams.S3.Secure, st.Streams.S3.Bucket, st.Streams.S3.Region)
			if err != nil {
				panic(err.Error())
			}
		} else {
			// Use a hardcoded access/secret key combo
			fstore, err = store.NewS3Store(st.Streams.S3.Endpoint, st.Streams.S3.AccessKey, st.Streams.S3.SecretKey, st.Streams.S3.Secure, st.Streams.S3.Bucket, st.Streams.S3.Region)
			if err != nil {
				panic(err.Error())
			}
		}
	case "azure":
		fstore, err = store.NewAzureStore(st.Streams.Azure.Endpoint, st.Streams.Azure.Container, st.Streams.Azure.StorageAccount)
		if err != nil {
			panic(err.Error())
		}
	case "local":
		// local file store
		fstore, err = store.NewLocalStore(st.Streams.Local.Path)
		if err != nil {
			panic(err.Error())
		}
	default:
		panic("No STORE_BACKEND configured.")
	}
	if st.Streams.Cache.SizeBytes > 1048576 {
		// wrap the previous store with an in-memory cache
		// defined in MB
		fstore, err = store.NewDataCache(int(st.Streams.Cache.SizeBytes/1048576), int(st.Streams.Cache.TTLSeconds), int(st.Streams.Cache.Shards), fstore)
		if err != nil {
			panic(err.Error())
		}
	}

	// Conditionally enable storing files with a XOR cipher to avoid AV detections
	// If false, this is transparent barring checks to see if files had previously been written with XORing
	// and parsing those
	fstore = store.NewXORStore(fstore, st.Streams.XOREncoding)

	identifier, err := identify.NewIdentifier()
	if err != nil {
		panic(err.Error())
	}

	return &Streams{
		Store:      fstore,
		identifier: identifier,
	}
}
