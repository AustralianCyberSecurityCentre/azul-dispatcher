package pipeline_consume

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/settings"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/msginflight"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
)

type CacheHit struct {
	hit map[string]bool
}

func calculateSecurityResult(authorSecurity, eventSecurity string) (bool, error) {
	cmd := exec.Command("azul-security", "can-access", authorSecurity, eventSecurity)
	cmd.Env = os.Environ()
	out, err := cmd.Output()
	if err != nil {
		return false, fmt.Errorf("bad commandline execution: %v", err)
	}
	simplified_output := strings.ToLower(strings.TrimSpace(string(out)))
	if simplified_output == "true" {
		return true, nil
	} else if simplified_output == "false" {
		return false, nil
	}
	return false, fmt.Errorf("bad security output: %s", out)
}

type FilterSecurity struct {
	CachedSecurityResults map[string]CacheHit
}

func (p *FilterSecurity) GetName() string { return "FilterSecurity" }

// Filter if this message has a security classification that exceeds what is allowed
func (p *FilterSecurity) ConsumeMod(message *msginflight.MsgInFlight, meta *consumer.ConsumeParams) (string, *msginflight.MsgInFlight) {
	if len(meta.MaxSecurity) == 0 {
		return "", message
	}
	binary, ok := message.GetBinary()
	var security string
	if !ok {
		download, ok := message.GetDownload()
		if !ok {
			return "", message
		}
		security = download.Source.Security
	} else {
		security = binary.Source.Security
	}
	if len(security) == 0 {
		return "", message
	}
	// Check cache and do comparison.
	var result bool
	var err error
	userSecurityHit, ok := p.CachedSecurityResults[meta.MaxSecurity]
	if !ok {
		result, err = calculateSecurityResult(meta.MaxSecurity, security)
		if err != nil {
			bedSet.Logger.Error().Err(err).Msg("Unable to provide security filtering.")
		}
		p.CachedSecurityResults[meta.MaxSecurity] = CacheHit{
			hit: map[string]bool{security: result},
		}
	} else {
		result, ok = userSecurityHit.hit[security]
		if !ok {
			result, err = calculateSecurityResult(meta.MaxSecurity, security)
			if err != nil {
				bedSet.Logger.Error().Err(err).Msg("Unable to provide security filtering.")
			}
			userSecurityHit.hit[security] = result
		}
	}
	// check if security allows message to continue.
	if result {
		return "", message
	}
	return "max_security", nil
}
