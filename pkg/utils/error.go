package utils

var (
	ErrNotFound = New(NotFoundErrorCode, "not found")

	ErrNoMetaDBRegistered = New(NoMetaDBRegisteredErrorCode, "no meta db registered")

	ErrWriterStopped = New(WriterStoppedErrorCode, "writer stopped")

	ErrReadInvalidHeader = New(ReadInvalidHeaderErrorCode, "invalid header")

	ErrAwsS3 = New(AwsS3ErrorCode, "aws s3 error")

	ErrWriterRecovey = New(WriterRecoveryErrorCode, "writer recovery error")
)

const (
	UnknownErrorCode            = -1
	NotFoundErrorCode           = 1000
	NoMetaDBRegisteredErrorCode = 1001
	WriterStoppedErrorCode      = 1002
	ReadInvalidHeaderErrorCode  = 1003
	AwsS3ErrorCode              = 1004
	WriterRecoveryErrorCode     = 1005
)

func New(code int, text string) error {
	return &ErrorCode{code, text}
}

type ErrorCode struct {
	code int
	s    string
}

func (e *ErrorCode) Error() string {
	return e.s
}

func (e *ErrorCode) Code() int {
	return e.code
}

func ErrorToErrorCode(err error) *ErrorCode {
	if err == nil {
		return nil
	}

	errorCode, ok := err.(*ErrorCode)
	if ok {
		return errorCode
	}

	return New(UnknownErrorCode, err.Error()).(*ErrorCode)
}
