package settings

import (
	"path"

	zlog "github.com/rs/zerolog/log"
	"gopkg.in/natefinch/lumberjack.v2"
)

// channels that log lines to files
var ChLogRestapiOk chan []byte
var ChLogRestapiErr chan []byte
var ChLogProducerErr chan []byte
var ChLogConsumerErr chan []byte

// start a new rotating logger that routes through a channel for performance
func makeFileLogger(filename string) chan []byte {
	// lumberjack lets us rotate log files automatically
	log := &lumberjack.Logger{
		Filename:   filename,
		MaxSize:    2, // megabytes
		MaxBackups: 3,
		MaxAge:     28,    //days
		Compress:   false, // disabled by default
	}
	ch := make(chan []byte, 20)
	go func() {
		var err error
		for line := range ch {
			if len(line) == 0 {
				continue
			}
			// ensure a newline in logged message
			combined := append(line, []byte("\n")...)
			_, err = log.Write(combined)
			if err != nil {
				zlog.Warn().Int("bytes", len(combined)).Str("file", filename).Msg("could not write restapi log line to file")
			}
		}
	}()
	return ch
}

// create all required loggers
func createFileLoggers(logpath string) {
	ChLogRestapiOk = makeFileLogger(path.Join(logpath, "restapi.ok.log"))
	ChLogRestapiErr = makeFileLogger(path.Join(logpath, "restapi.err.log"))
	ChLogConsumerErr = makeFileLogger(path.Join(logpath, "consumer.err.log"))
	ChLogProducerErr = makeFileLogger(path.Join(logpath, "producer.err.log"))
}
