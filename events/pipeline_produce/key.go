package pipeline_produce

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
)

// Key generates a unique key id for a given event message based on its content.
// For statuses it creates a key unique to the original task provided to plugins.
// Caller is expected to validate incoming data is correct
// If you throw invalid json in or json that doesn't conform, you might get a key out anyway.
func key(msg *msginflight.MsgInFlight) ([]byte, error) {
	buf, err := keyDescriptor(msg)
	if err != nil {
		return nil, err
	}
	hash := md5.Sum(buf)
	key := hex.EncodeToString(hash[:])
	return []byte(key), nil
}

func keyDescriptor(msg *msginflight.MsgInFlight) ([]byte, error) {
	switch ev := msg.Event.(type) {
	case *events.StatusEvent:
		// Entity.id is the dequeued_id of the entity which is originally created in notifier.
		// It is provided back in the entity.id by azul_runner.
		// Author name is used to ensure multi-plugins don't have the same key value.
		keyString := fmt.Sprintf("%s.%s", ev.Entity.Input.Dequeued, ev.Author.Name)
		return []byte(keyString), nil
	case *events.PluginEvent:
		// id is name + version
		keyString := strings.Join([]string{
			ev.Entity.Name,
			ev.Entity.Version,
		}, ".")
		return []byte(keyString), nil
	case *events.InsertEvent:
		// parent + child + author
		keyString := strings.Join([]string{
			ev.Entity.ParentSha256,
			ev.Entity.Child.Sha256,
			ev.Entity.ChildHistory.Author.Name,
		}, ".")
		return []byte(keyString), nil
	case *events.BinaryEvent:
		// IDs collide if everything is the same except for ancestors in source.path that are not the parent or first binary in the chain.
		// i.e. deduplicate docs that only differ because two plugins extracted the same file at a higher level.
		// Timing of execution is irrelevant.
		// Output of a plugin against a binary is assumed to be identical.
		buf := make([]byte, 0, 1024)
		// event, entity type, entity id already included from source.path, so skipped at top level
		buf = append(buf, ev.Source.Name...)
		buf = append(buf, byte('.'))
		buf = append(buf, ev.Source.Security...)
		buf = append(buf, byte('.'))
		// do timestamps require any normalisation, eg. timezone/offsets?
		tz := ev.Source.Timestamp.Format(time.RFC3339)
		if len(tz) > 19 {
			buf = append(buf, tz[:19]...)
			buf = append(buf, byte('.'))
		}
		// include source ref fields (in order) in case multiple per source time
		// i.e. any change in refs is considered a new/unique event
		kv := []string{}
		refs := ev.Source.References
		for k, v := range refs {
			kv = append(kv, k+"."+v)
		}
		sort.Strings(kv)
		for i := range kv {
			buf = append(buf, kv[i]...)
			buf = append(buf, byte('.'))
		}

		// include source settings in case settings differ between submissions
		kv = []string{}
		submit_settings := ev.Source.Settings
		for k, v := range submit_settings {
			kv = append(kv, k+"."+v)
		}
		sort.Strings(kv)
		for i := range kv {
			buf = append(buf, kv[i]...)
			buf = append(buf, byte('.'))
		}

		if len(ev.Source.Path) == 0 {
			return nil, errors.New("bad path, no items found")
		}

		// find the first, parent and current nodes on the path
		var first *events.EventSourcePathNode
		var parent *events.EventSourcePathNode
		var current *events.EventSourcePathNode
		securitiesMap := map[string]bool{} // basically a set
		path := ev.Source.Path
		for i := range path {
			if first != nil {
				first = &path[i]
			}
			if current != nil {
				parent = current
			}
			current = &path[i]
			sec := path[i].Author.Security
			if len(sec) > 0 {
				securitiesMap[sec] = true
			}
		}

		// add unique securities seen on path
		securities := []string{}
		for k := range securitiesMap {
			securities = append(securities, k)
		}
		sort.Strings(securities)
		for i := range securities {
			buf = append(buf, securities[i]...)
			buf = append(buf, byte('.'))
		}

		// For first, parent and current nodes only.
		// Do not include author version as we want those to collide - last seen wins
		for _, v := range []*events.EventSourcePathNode{first, parent, current} {
			if v == nil {
				continue
			}
			buf = append(buf, v.Author.Name...)
			buf = append(buf, byte('.'))
			buf = append(buf, v.Action...)
			buf = append(buf, byte('.'))
			buf = append(buf, v.Sha256...)
			buf = append(buf, byte('.'))
		}
		return buf, nil
	default:
		// For insert and delete events (and any others) use random bytes.
		// We are not concerned about enforcing deduplication.
		token := make([]byte, 32)
		// from docs - Read fills b with cryptographically secure random bytes. It never returns an error, and always fills b entirely.
		_, err := rand.Read(token)
		if err != nil {
			panic(fmt.Errorf("failed to generate random data %s", err.Error()))
		}
		return token, nil
	}
}
