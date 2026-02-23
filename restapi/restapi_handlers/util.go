package restapi_handlers

import (
	"errors"
	"fmt"
	"log"
	"runtime/debug"

	"github.com/gin-gonic/gin"
	"github.com/goccy/go-json"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/models"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/settings"
)

// Error replies to the request with the specified error message and HTTP code.
// It does not otherwise end the request; the caller should ensure no further
// writes are done to the gin context.
func JSONErrorWithEnum(c *gin.Context, code int, title string, baseErr error, errorEnum models.ErrorStringEnum, errorParams map[string]string){
	if baseErr == nil {
		baseErr = errors.New("no error provided")
	}
	// print error to logs
	if code >= 500 && code <= 599 {
		// print traceback so we might be able to determine more about where specifically the error occurred
		debug.PrintStack()
		// even though we use Stack(), doesn't print a stacktrace
		bedSet.Logger.Err(baseErr).Stack().Int("code", code).Str("title", title).Msg("internal restapi error")
	}

	// generate standard error response
	response := models.Error{Status: fmt.Sprint(code), Title: title, Detail: baseErr.Error(), ErrorEnum: errorEnum}
	out, err := json.Marshal(response)
	if err != nil {
		bedSet.Logger.Err(err).Int("code", code).Str("title", title).Str("detail", baseErr.Error()).
			Msg("restapi failed to return json error response")
	}

	// update response
	c.Writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	c.Writer.Header().Set("X-Content-Type-Options", "nosniff")
	c.Writer.WriteHeader(code)
	_, err = c.Writer.Write(out)
	c.Writer.Flush()
	if err != nil {
		log.Println(err)
	}
}

// Error replies to the request with the specified error message and HTTP code.
// It does not otherwise end the request; the caller should ensure no further
// writes are done to the gin context.
func JSONError(c *gin.Context, code int, title string, baseErr error) {
	JSONErrorWithEnum(c, code, title, baseErr, models.ErrorStringEnumUnset, map[string]string{})
}
