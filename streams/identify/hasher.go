package identify

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"log"
	"os"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/settings"
	ssdeep "github.com/dutchcoders/gossdeep"
	"github.com/glaslos/tlsh"
)

const BYTE_BUFFER_SIZE = 1024 * 10

func hashFilePath(filepath string) (*events.BinaryEntityDatastream, error) {
	h := NewHasher(false)
	fileHandler, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open a file in doWork with path %s with error %s", filepath, err.Error())
	}
	defer fileHandler.Close()
	buf := make([]byte, BYTE_BUFFER_SIZE)
	var readBytes int

	for {
		readBytes, err = fileHandler.Read(buf)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read bytes when calculating hash %s", err.Error())
		}
		if readBytes == 0 {
			break
		}
		err = h.Write(buf[:readBytes])
		if err == io.EOF {
			break
		}
	}
	return h.Cook()
}

// Hasher allows for processing a stream of data rather than a file from disk
type Hasher struct {
	minimal bool
	size    int
	sha256  hash.Hash
	sha512  hash.Hash
	sha1    hash.Hash
	md5     hash.Hash
	ssdeep  *ssdeep.FuzzyState
	tlsh    *tlsh.TLSH
}

func NewHasher(minimal bool) *Hasher {
	ssdeepBuf, err := ssdeep.New()
	if err != nil {
		// initialising this should never fail
		panic(err)
	}
	return &Hasher{
		minimal: minimal,
		size:    0,
		sha512:  sha512.New(),
		sha256:  sha256.New(),
		sha1:    sha1.New(),
		md5:     md5.New(),
		ssdeep:  ssdeepBuf,
		tlsh:    tlsh.New(),
	}
}

func (h *Hasher) Write(buf []byte) error {
	var err error
	var written int
	genericError := "failed to calculate the %s hash with error %s"
	written, err = h.sha256.Write(buf)
	h.size += written
	if err != nil {
		return fmt.Errorf(genericError, "sha256", err.Error())
	}
	if !h.minimal {
		_, err = h.sha512.Write(buf)
		if err != nil {
			return fmt.Errorf(genericError, "sha512", err.Error())
		}
		_, err = h.sha1.Write(buf)
		if err != nil {
			return fmt.Errorf(genericError, "sha1", err.Error())
		}
		_, err = h.md5.Write(buf)
		if err != nil {
			return fmt.Errorf(genericError, "md5", err.Error())
		}
		// Complex hash types
		_, err = h.tlsh.Write(buf)
		if err != nil {
			return fmt.Errorf(genericError, "tlsh", err.Error())
		}
		err = h.ssdeep.Update(string(buf))
		if err != nil {
			return fmt.Errorf(genericError, "ssdeep", err.Error())
		}

	}
	return nil
}

func (h *Hasher) Cook() (*events.BinaryEntityDatastream, error) {
	var err error
	md := &events.BinaryEntityDatastream{}
	md.Size = uint64(h.size)
	md.Sha256 = fmt.Sprintf("%x", h.sha256.Sum(nil))
	if !h.minimal {
		md.Sha512 = fmt.Sprintf("%x", h.sha512.Sum(nil))
		md.Sha1 = fmt.Sprintf("%x", h.sha1.Sum(nil))
		md.Md5 = fmt.Sprintf("%x", h.md5.Sum(nil))
		// if we have enough bytes, calculate ssdeep
		// files smaller than 4kb may produce an ssdeep hash, but it can't be used for anything useful
		hash_ssdeep := ""
		if h.size >= 4096 {
			hash_ssdeep, err = h.ssdeep.Digest()
			if err != nil {
				bedSet.Logger.Warn().Msgf("Failed to calculate ssdeep with error: %v", err)
				return nil, fmt.Errorf("failed to calculate ssdeep with internal error: %v", err)
			}
		}
		md.Ssdeep = hash_ssdeep

		hash_tlsh := ""
		// If we don't have at least 50 bytes can't calculate tlsh
		if h.size > 50 {
			// Manually adding the T1 here because the golang library hasn't added it.
			// the hash value is the same as the python equivalent with the exception of the leading T1.
			hash_tlsh = "T1" + hex.EncodeToString(h.tlsh.Sum(nil))
			// If the TLSH isn't 72 characters it's invalid and it should be set as such.
			if len(hash_tlsh) < 72 {
				log.Printf("")
				bedSet.Logger.Warn().Msgf("Unable to calculate the TLSH value for binary '%s'", md.Sha256)
				hash_tlsh = ""
			}
		}
		md.Tlsh = hash_tlsh
	}

	if md.Size == 0 {
		md.Mime = "inode/x-empty"
		md.Magic = "empty"
		md.FileFormat = "unknown"
		md.FileExtension = ""
	}
	return md, nil
}
