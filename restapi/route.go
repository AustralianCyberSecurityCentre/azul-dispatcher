package restapi

import (
	"context"

	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/streams"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Dispatcher struct {
	Router *gin.Engine
	event  *events.Events
	stream *streams.Streams
}

// response to hitting '/' on the server
func GetRoot(c *gin.Context) {
	c.Writer.Header().Set("Content-Type", "text/plain")
	_, err := c.Writer.Write([]byte("Azul Dispatcher"))
	if err != nil {
		bedSet.Logger.Err(err).Msg("get root")
	}
}

// Basic middleware to log errors.
func ErrorLoggerMiddleware(c *gin.Context) {
	if c == nil {
		bedSet.Logger.Error().Msg("gin error, couldn't provide error info as context was nil.")
		return
	}
	c.Next()

	for _, err := range c.Errors {
		if c.Request == nil || c.Request.URL == nil {
			bedSet.Logger.Error().Err(err).Msg("gin error, limited detail was Request or Request URL was nil.")
		} else {
			bedSet.Logger.Error().Err(err).Msgf("gin error on route %s %s with query params %v", c.Request.Method, c.Request.URL, c.Request.URL.Query())
		}
	}
}

func NewDispatcher(prov provider.ProviderInterface, kvprov *kvprovider.KVMulti, ctx context.Context) *Dispatcher {
	var stream = streams.NewStreams()
	var event = events.NewEvents(prov, kvprov, stream.Store, ctx)

	event.InitialiseKafka()
	gin.SetMode(gin.ReleaseMode) // don't print route list on start

	bedSet.Logger.Info().Msg("Start Dispatcher RestAPI")
	router := gin.New()
	router.Use(ErrorLoggerMiddleware)
	// universally post binary/status/plugin/etc events to dispatcher
	lpath := "/api/v2/event"
	router.POST(lpath, MetricHandler(lpath, event.PostEvent))
	// retrieve events with no side-effects
	lpath = "/api/v2/event/:model/passive"
	router.GET(lpath, MetricHandler(lpath, event.GetEventsPassive))
	// retrieve events with expectation that status results will be posted back
	lpath = "/api/v2/event/:model/active"
	router.GET(lpath, MetricHandler(lpath, event.GetEventsActiveImplicit))
	// simulate all plugin filters on a single event, to see if they would silently skip
	lpath = "/api/v2/event/simulate"
	router.POST(lpath, MetricHandler(lpath, event.PostEventSimulate))

	// post new binary stream
	lpath = "/api/v3/stream/:source/:label"
	router.POST(lpath, MetricHandler(lpath, stream.PostStream))
	// get binary blob with matching hash
	lpath = "/api/v3/stream/:source/:label/:hash"
	router.GET(lpath, MetricHandler(lpath, stream.GetData))
	// check binary blob in system exists with matching hash
	router.HEAD(lpath, MetricHandler(lpath, stream.HasData))
	// limit delete to specific instances of dispatcher
	// i.e. plugins should not be able to delete
	if st.Streams.APIAllowDelete {
		router.DELETE(lpath, MetricHandler(lpath, stream.DeleteData))
	}
	// copy binary blob from source A to source B
	lpath = "/api/v3/stream/:sourceA/:sourceB/:label/:hash"
	router.PATCH(lpath, MetricHandler(lpath, stream.CopyData))

	// base response
	router.GET("/", GetRoot)

	// using alternate router, need to redirect
	// memory monitoring, required for us to debug memory usage in dispatcher
	pprof.Register(router, "debug/pprof")

	// prometheus metrics endpoint
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	return &Dispatcher{router, event, stream}
}

func (dp *Dispatcher) Stop() {
	bedSet.Logger.Info().Msg("stopping dispatcher restapi and cleaning up data structures")
	dp.event.Stop()
	bedSet.Logger.Info().Msg("stopped dispatcher restapi")
	dp.stream.Close()
	// dp.stream.Stop()
}
