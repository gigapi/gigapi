package utils

type IGigapiError interface {
	error
	Code() int
}

type GigapiError struct {
	message string
	code    int
}

func (g *GigapiError) Error() string {
	return g.message
}

func (g *GigapiError) Code() int {
	return g.code
}

func NewGigapiError(message string, code int) *GigapiError {
	return &GigapiError{
		message: message,
		code:    code,
	}
}
