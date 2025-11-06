package restapi

import (
	"fmt"
	"net/http"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/prom"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/gin-gonic/gin"
	"github.com/goccy/go-json"
	"github.com/rs/zerolog/log"
)

type loggingResponseWriter struct {
	gin.ResponseWriter
	statusCode   int
	responseBody []byte
}

// https://stackoverflow.com/questions/53272536/how-do-i-get-response-statuscode-in-golang-middleware
func NewLoggingResponseWriter(c *gin.Context) *loggingResponseWriter {
	return &loggingResponseWriter{c.Writer, http.StatusOK, nil}
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
	lrw.Flush()
}

func (lrw *loggingResponseWriter) Write(data []byte) (int, error) {
	outB, err := lrw.ResponseWriter.Write(data)
	if lrw.statusCode >= 400 {
		// collect error response for logging
		lrw.responseBody = append(lrw.responseBody, data...)
	}
	return outB, err
}

type RestapiLogLine struct {
	Time                string          `json:"time"`
	DurationS           string          `json:"duration_s"`
	Status              int             `json:"status"`
	Method              string          `json:"method"`
	Route               string          `json:"route"`
	Path                string          `json:"path"`
	Query               string          `json:"query"`
	Remote              string          `json:"remote"`
	Useragent           string          `json:"user_agent"`
	ResponseBodyInvalid bool            `json:"response_body_invalid,omitempty"`
	ResponseBody        json.RawMessage `json:"response_body,omitempty"`
}

// MetricHandler measures time taken to respond and logs with prometheus
// can't get the template path out of the request so must be supplied on startup
func MetricHandler(tpath string, fn gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		lrw := NewLoggingResponseWriter(c)

		start := time.Now()
		fn(c)
		elapsed := time.Since(start).Seconds()
		prom.RestapiTimes.WithLabelValues(c.Request.Method, tpath).Observe(elapsed)
		prom.RestapiCodes.WithLabelValues(c.Request.Method, tpath, fmt.Sprintf("%v", lrw.statusCode)).Add(1)

		// track restapi access in a log file
		// encode as json not logfmt to not worry about bad client characters
		logLineStruct := RestapiLogLine{
			Time:         start.Format(time.RFC3339),
			DurationS:    fmt.Sprintf("%.4f", elapsed),
			Status:       lrw.statusCode,
			Method:       c.Request.Method,
			Route:        tpath,
			Path:         c.Request.URL.Path,
			Query:        c.Request.URL.RawQuery,
			Remote:       c.Request.RemoteAddr,
			Useragent:    c.Request.UserAgent(),
			ResponseBody: lrw.responseBody,
		}

		logline, err := json.Marshal(logLineStruct)
		if err != nil {
			log.Warn().Err(err).Str("response", string(logLineStruct.ResponseBody)).Msg("could not marshal restapi log line during metric, dropping body")
			logLineStruct.ResponseBody = nil
			logLineStruct.ResponseBodyInvalid = true
			logline, err = json.Marshal(logLineStruct)
			if err != nil {
				log.Error().Err(err).Msg("could not marshal restapi log line during metric, total failure")
				return
			}
		}

		// log to file for loki collection - depending on response code
		if lrw.statusCode < 400 {
			st.ChLogRestapiOk <- logline
		} else {
			st.ChLogRestapiErr <- logline
		}
	}
}
