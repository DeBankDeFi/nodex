package utils

var (
	ErrNoMetaDBRegistered = New(NoMetaDBRegisteredErrorCode, "no meta db registered")

	ErrMetaDBAlreadyRegistered = New(MetaDBAlreadyRegisteredErrorCode, "meta db already registered")

	ErrWriterStopped = New(WriterStoppedErrorCode, "writer stopped")

	ErrReadInvalidHeader = New(ReadInvalidHeaderErrorCode, "invalid header")

	ErrAwsS3 = New(AwsS3ErrorCode, "aws s3 error")

	ErrWriterRecovey = New(WriterRecoveryErrorCode, "writer recovery error")

	ErrStreamNotInit = New(StreamNotInitErrorCode, "stream not init")
)

const (
	UnknownErrorCode                 = -1
	NoMetaDBRegisteredErrorCode      = 41001
	WriterStoppedErrorCode           = 41002
	ReadInvalidHeaderErrorCode       = 41003
	AwsS3ErrorCode                   = 41004
	WriterRecoveryErrorCode          = 41005
	RemoteErrorCode                  = 41006
	MetaDBAlreadyRegisteredErrorCode = 41007
	BroadcasterErrorCode             = 41008
	StreamNotInitErrorCode           = 41009
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
