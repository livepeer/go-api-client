package metrics

import (
	"time"
)

type APIRecorder interface {
	APIRequest(name string, duration time.Duration, err error)
}

type APIRequestRecorderFunc func(name string, duration time.Duration, err error)

func (f APIRequestRecorderFunc) APIRequest(name string, duration time.Duration, err error) {
	f(name, duration, err)
}
