/*
Package dedupe contains filters for deduplication of event processing.

It is heavily inspired from Jeffrey Hodge's OppoBloom Filter:
https://github.com/jmhodges/opposite_of_a_bloom_filter

It uses a hashtable (utilising xxHash64) to construct a fixed-size
lookup with known collision rates to determine if a key has been seen
previously.

As opposed to a bloom filter, this lookup cache will not falsely report an
entry as duplicate (only up to 64 bit collision rate) but can return
false negatives up to the key size collision rate.

Examples:

* 32MB sized cache
** Key size: 22 bits
** Entries: 4mil
** False negative rate: 1:3k
** False positive rate: 1:6bil

* 256MB sized cache
** Entries: 32mil
** Key size: 25 bits
** False negative rate: 1:8k
** False positive rate: 1:6bil

* 8GB sized cache
** Entries: 1bil
** Key Size: 30 bits
** False negative rate: 1:46k
** False positive rate: 1:6bil

* 32GB sized cache
** Entries: 4bil
** Key Size: 32 bits
** False negative rate: 1:92k
** False positive rate: 1:6bil

False negatives result in unnecessary processing (meh)
False positives result in missed/lost processing (bad)

While the error rate seems high for the memory consumption, keep in mind that
duplicates tend to cluster during processing and so effective rate is much
higher than the total system event count.
*/
package dedupe
