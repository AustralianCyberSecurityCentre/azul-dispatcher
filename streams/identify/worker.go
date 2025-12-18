package identify

import (
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
	bed_identify "github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/identify"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/prom"
)

type Worker struct {
	work     chan Work
	identify *bed_identify.Identify
}

func NewWorker(work chan Work) (*Worker, error) {
	ider, err := bed_identify.NewIdentify()
	if err != nil {
		return nil, err
	}
	return &Worker{work, ider}, nil
}

func (worker *Worker) doWork(w Work) error {
	var err error
	md := w.stream
	md.IdentifyVersion = worker.identify.Version
	md.Label = events.DataLabelContent

	// drop non ascii characters from magic/mime
	id, err := worker.identify.Find(w.filePath)
	md.Mime = id.Mime
	md.Magic = id.Magic
	md.FileFormat = id.FileFormat
	md.FileFormatLegacy = id.FileFormatLegacy
	md.FileExtension = id.FileExtension
	if err != nil {
		return err
	}
	if md.FileFormat == "unknown" {
		// If we didn't match a file type, give prometheus the failing mime and magic
		// the magic might be long, but this is necessary to build a good pattern in many cases
		prom.IdentifyFail.WithLabelValues(md.Mime, md.Magic).Inc()
	}

	return nil
}

func (worker *Worker) Start() {
	for {
		w, success := <-worker.work
		if !success {
			break
		}
		err := worker.doWork(w)
		w.res <- err
	}
}
