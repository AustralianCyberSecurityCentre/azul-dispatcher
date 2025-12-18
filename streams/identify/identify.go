/*
package identify provides ability to extract common file, magic and mime type information from byte buffers.
*/
package identify

import (
	"log"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
)

const EmbeddedContentMaxBytes = 100
const ModelWorkers = 10

type Work struct {
	// filePath to identify
	filePath string
	stream   *events.BinaryEntityDatastream
	// channel to send result back on
	res chan error
}

/* Group of identifier workers and a controlling channel. */
type Identifier struct {
	work chan Work
}

func NewIdentifier() (Identifier, error) {
	f := Identifier{make(chan Work, ModelWorkers)}
	for i := 0; i < ModelWorkers; i++ {
		worker, err := NewWorker(f.work)
		if err != nil {
			return Identifier{}, nil
		}
		go worker.Start()
	}
	return f, nil
}

func (f *Identifier) Close() {
	close(f.work)
}

/* Calculate metadata for supplied bytes */
func (f *Identifier) Identify(filePath string, meta *events.BinaryEntityDatastream) error {
	var err error
	w := Work{filePath, meta, make(chan error)}
	f.work <- w
	err = <-w.res
	if err != nil {
		log.Printf("failed to identify file format: %v", err)
		return err
	}
	return nil
}

// Directly perform hashing and identify over a filepath.
func (f *Identifier) HashAndIdentify(filePath string) (*events.BinaryEntityDatastream, error) {
	metadata, err := hashFilePath(filePath)
	if err != nil {
		return nil, err
	}
	err = f.Identify(filePath, metadata)
	if err != nil {
		return nil, err
	}
	return metadata, nil
}
