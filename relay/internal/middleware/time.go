package middleware

import "time"

// timeNowMs returns the current wall-clock time in milliseconds. Isolated in
// its own file so rate-limit tests can swap nowUnixMs without changing the
// rest of the package.
func timeNowMs() int64 {
	return time.Now().UnixMilli()
}
