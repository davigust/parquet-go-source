package azblob

import (
	"context"
	"errors"
	"io"
	"net/url"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/xitongsys/parquet-go/source"
)

// blockBlob is ParquetFile for azblob
type blockBlob struct {
	ctx          context.Context
	url          *url.URL
	credential   azblob.Credential
	blockBlobURL *azblob.BlockBlobURL

	// write-related fields
	writeDone     chan error
	pipeReader    *io.PipeReader
	pipeWriter    *io.PipeWriter
	writerOptions WriterOptions

	// read-related fields
	offset        int64
	whence        int
	fileSize      int64
	readerOptions ReaderOptions
}

var (
	errWhence         = errors.New("Seek: invalid whence")
	errInvalidOffset  = errors.New("Seek: invalid offset")
	errReadNotOpened  = errors.New("Read: url not opened")
	errWriteNotOpened = errors.New("Write url not opened")
)

type ReaderOptions struct {
	// HTTPSender configures the sender of HTTP requests
	HTTPSender pipeline.Factory

	RetryOptions azblob.RetryReaderOptions
}

type WriterOptions struct {
	BlockSize int

	// HTTPSender configures the sender of HTTP requests
	HTTPSender pipeline.Factory
}

// NewAzBlobFileWriter creates an Azure Blob FileWriter, to be used with NewParquetWriter
func NewAzBlobFileWriter(ctx context.Context, URL string, credential azblob.Credential, options WriterOptions) (source.ParquetFile, error) {
	file := &blockBlob{
		ctx:           ctx,
		credential:    credential,
		writerOptions: options,
	}

	return file.Create(URL)
}

// NewAzBlobFileReader creates an Azure Blob FileReader, to be used with NewParquetReader
func NewAzBlobFileReader(ctx context.Context, URL string, credential azblob.Credential, options ReaderOptions) (source.ParquetFile, error) {
	file := &blockBlob{
		ctx:           ctx,
		credential:    credential,
		readerOptions: options,
	}

	return file.Open(URL)
}

// Seek tracks the offset for the next Read. Has no effect on Write.
func (s *blockBlob) Seek(offset int64, whence int) (int64, error) {
	if whence < io.SeekStart || whence > io.SeekEnd {
		return 0, errWhence
	}

	if s.fileSize > 0 {
		switch whence {
		case io.SeekStart:
			if offset < 0 || offset > s.fileSize {
				return 0, errInvalidOffset
			}
		case io.SeekCurrent:
			offset += s.offset
			if offset < 0 || offset > s.fileSize {
				return 0, errInvalidOffset
			}
		case io.SeekEnd:
			if offset > -1 || -offset > s.fileSize {
				return 0, errInvalidOffset
			}
		}
	}

	s.offset = offset
	s.whence = whence
	return s.offset, nil
}

// Read up to len(p) bytes into p and return the number of bytes read
func (s *blockBlob) Read(p []byte) (n int, err error) {
	if s.blockBlobURL == nil {
		return 0, errReadNotOpened
	}

	if s.fileSize > 0 && s.offset >= s.fileSize {
		return 0, io.EOF
	}

	count := int64(len(p))
	resp, err := s.blockBlobURL.Download(s.ctx, s.offset, count, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return 0, err
	}
	if s.fileSize < 0 {
		s.fileSize = resp.ContentLength()
	}

	toRead := s.fileSize - s.offset
	if toRead > count {
		toRead = count
	}

	body := resp.Body(s.readerOptions.RetryOptions)
	bytesRead, err := io.ReadFull(body, p[:toRead])
	if err != nil {
		return 0, err
	}

	s.offset += int64(bytesRead)

	return bytesRead, nil
}

// Write len(p) bytes from p
func (s *blockBlob) Write(p []byte) (n int, err error) {
	if s.blockBlobURL == nil {
		return 0, errWriteNotOpened
	}

	bytesWritten, writeError := s.pipeWriter.Write(p)
	if writeError != nil {
		s.pipeWriter.CloseWithError(err)
		return 0, writeError
	}

	return bytesWritten, nil
}

// Close signals write completion and cleans up any
// open streams. Will block until pending uploads are complete.
func (s *blockBlob) Close() error {
	var err error

	if s.pipeWriter != nil {
		if err = s.pipeWriter.Close(); err != nil {
			return err
		}

		// wait for pending uploads
		err = <-s.writeDone
	}

	return err
}

// Open creates a new block blob to perform reads
func (s *blockBlob) Open(URL string) (source.ParquetFile, error) {
	var u *url.URL
	if len(URL) == 0 {
		// ColumnBuffer passes in an empty string for name
		u = s.url
	} else {
		var err error
		if u, err = url.Parse(URL); err != nil {
			return s, err
		}
	}

	blobURL := azblob.NewBlockBlobURL(*u, azblob.NewPipeline(s.credential, azblob.PipelineOptions{HTTPSender: s.readerOptions.HTTPSender}))

	fileSize := int64(-1)
	props, err := blobURL.GetProperties(s.ctx, azblob.BlobAccessConditions{})
	if err == nil {
		fileSize = props.ContentLength()
	}

	pf := &blockBlob{
		ctx:           s.ctx,
		url:           u,
		credential:    s.credential,
		blockBlobURL:  &blobURL,
		fileSize:      fileSize,
		readerOptions: s.readerOptions,
	}

	return pf, nil
}

// Create a new blob url to perform writes
func (s *blockBlob) Create(URL string) (source.ParquetFile, error) {
	var u *url.URL
	if len(URL) == 0 {
		// ColumnBuffer passes in an empty string for name
		u = s.url
	} else {
		var err error
		if u, err = url.Parse(URL); err != nil {
			return s, err
		}
	}

	blobURL := azblob.NewBlockBlobURL(*u, azblob.NewPipeline(s.credential, azblob.PipelineOptions{HTTPSender: s.writerOptions.HTTPSender}))

	// get account properties to validate credentials
	if _, err := blobURL.GetAccountInfo(s.ctx); err != nil {
		return nil, err
	}

	pf := &blockBlob{
		ctx:           s.ctx,
		url:           u,
		credential:    s.credential,
		blockBlobURL:  &blobURL,
		writerOptions: s.writerOptions,
		writeDone:     make(chan error),
	}

	pf.pipeReader, pf.pipeWriter = io.Pipe()

	go func(ctx context.Context, blobURL *azblob.BlockBlobURL, o WriterOptions, reader io.Reader, readerPipeSource *io.PipeWriter, done chan error) {
		defer close(done)

		// upload data and signal done when complete
		_, err := azblob.UploadStreamToBlockBlob(ctx, reader, *blobURL, azblob.UploadStreamToBlockBlobOptions{BufferSize: o.BlockSize})
		if err != nil {
			readerPipeSource.CloseWithError(err)
		}

		done <- err
	}(pf.ctx, pf.blockBlobURL, pf.writerOptions, pf.pipeReader, pf.pipeWriter, pf.writeDone)

	return pf, nil
}
