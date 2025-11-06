package store

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"

	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
)

/* Store file on local filesystem. */
type StoreFilesystem struct {
	root string
}

// NewEmptyLocalStore returns a FileStorage wil no data.
// Intended for testing, as aborted tests may otherwise leave files on disk.
func NewEmptyLocalStore(root string) (FileStorage, error) {
	err := os.RemoveAll(root)
	if err != nil {
		return nil, err
	}
	return NewLocalStore(root)
}

func NewLocalStore(root string) (FileStorage, error) {
	err := os.MkdirAll(root, 0755)
	return &StoreFilesystem{root}, err
}

func (s *StoreFilesystem) GetRootPath() string {
	return s.root
}

func (s *StoreFilesystem) Put(source, label, id string, inFilePath string, fileSize int64) error {
	dirname := filepath.Join(s.root, source, label, id[0:1], id[1:2])
	err := os.MkdirAll(dirname, 0755)
	if err != nil {
		// log error, if this is a critical error it will be caught and returned below
		log.Println("Error creating dir")
		log.Println(err)
	}
	path := filepath.Join(dirname, id)
	if _, err := os.Stat(path); err == nil {
		// file was already on disk with same sha256 so abort the put
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		// failed to get file info for some reason but it exists
		// Log the error; if it's serious, we'll return error from WriteFile below
		log.Println("Error calling stat(" + path + ")")
		log.Println(err)
	}
	// trunc will rewrite the file if it exists
	destFile, err := os.OpenFile(path, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0640)
	if err != nil {
		return fmt.Errorf("could not save local file as local file couldn't be opened: %w", err)
	}
	defer destFile.Close()
	srcFile, err := os.Open(inFilePath)
	if err != nil {
		return fmt.Errorf("could not save local file because source file could not be opened: %w", err)
	}
	defer srcFile.Close()
	_, err = io.Copy(destFile, srcFile)
	return err
}

func (s *StoreFilesystem) Fetch(source, label, id string, offset int64, size int64) (DataSlice, error) {
	empty := NewDataSlice()
	path := filepath.Join(s.root, source, label, id[0:1], id[1:2], id)
	f, err := os.Open(path)
	if err != nil {
		e := fmt.Errorf("%w", &AccessError{msg: fmt.Sprintf("%v", err)})
		if os.IsNotExist(err) {
			e = fmt.Errorf("%w", &NotFoundError{})
		}
		return empty, e
	}

	// find out length of object
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return empty, fmt.Errorf("%w", &AccessError{msg: fmt.Sprintf("%v", err)})
	}
	// -ve or zero is read all
	if size <= 0 {
		size = stat.Size()
	}
	// treat -ve as relative to end
	if offset < 0 {
		offset = stat.Size() + offset
	}
	// still -ve (-ve offset was bigger than file)
	if offset < 0 {
		offset = 0
	}
	// offset after end of file
	if offset > 0 && offset >= stat.Size() {
		f.Close()
		return empty, fmt.Errorf("%w", &OffsetAfterEnd{msg: fmt.Sprintf("offset after EOF: %d", stat.Size())})
	}
	// requested more than available
	// should we error or be lenient?
	if offset+size > stat.Size() {
		size = stat.Size() - offset
	}
	_, err = f.Seek(offset, 0)
	if err != nil && err != io.EOF {
		f.Close()
		return empty, fmt.Errorf("%w", &ReadError{msg: fmt.Sprintf("%v", err)})
	}
	limitedReader := NewCloseWrapper(io.LimitReader(f, size), f)
	return DataSlice{limitedReader, offset, size, stat.Size()}, nil
}

func (s *StoreFilesystem) Exists(source, label, id string) (bool, error) {
	path := filepath.Join(s.root, source, label, id[0:1], id[1:2], id)
	if _, err := os.Stat(path); err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	} else {
		return false, fmt.Errorf("%w", &AccessError{msg: fmt.Sprintf("%v", err)})
	}
}

func (s *StoreFilesystem) Copy(sourceOld, labelOld, idOld, sourceNew, labelNew, idNew string) error {
	// default srcFilePath to just the object name/hash
	srcFilePath := filepath.Join(s.root, sourceOld, labelOld, idOld[0:1], idOld[1:2], idOld)
	existsUnderSource, err := s.Exists(sourceOld, labelOld, idOld)
	if err != nil {
		return fmt.Errorf("error locating source object %s", srcFilePath)
	}

	// check if the src object is under src/label/
	if !existsUnderSource {
		st.Logger.Warn().Msgf("Object %s not found for copy operation", srcFilePath)
		return nil
	}

	// Get file size for consistency with other APIs.
	file, err := os.Open(srcFilePath)
	if err != nil {
		return err
	}
	defer file.Close()
	file_info, err := file.Stat()
	if err != nil {
		return err
	}

	// Copy using existing function
	err = s.Put(sourceNew, labelNew, idNew, srcFilePath, file_info.Size())
	if err != nil {
		return err
	}

	return nil
}

func (s *StoreFilesystem) Delete(source, label, id string, ifOlderThan int64) (bool, error) {
	if ifOlderThan > 0 {
		return false, errors.New("local filesystem does not support ifOlderThan deletion")
	}
	path := filepath.Join(s.root, source, label, id[0:1], id[1:2], id)
	err := os.Remove(path)
	if err != nil {
		e := fmt.Errorf("%w", &AccessError{msg: fmt.Sprintf("%v", err)})
		if os.IsNotExist(err) {
			e = fmt.Errorf("%w", &NotFoundError{})
		}
		return false, e
	}
	return true, nil
}
