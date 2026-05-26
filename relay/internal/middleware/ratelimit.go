package middleware

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"github.com/adithya/relay/pkg/response"
)

// tokenBucketScript implements a leaky token bucket in a single round trip.
// Keys: [1] = bucket key
// ARGV: [1] = capacity, [2] = refill_rate per second, [3] = now (unix ms), [4] = requested tokens (usually 1)
// Returns: { allowed (0|1), remaining_tokens, retry_after_ms }
const tokenBucketScript = `
local key      = KEYS[1]
local cap      = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local now_ms   = tonumber(ARGV[3])
local req      = tonumber(ARGV[4])

local data = redis.call("HMGET", key, "tokens", "ts")
local tokens = tonumber(data[1])
local last   = tonumber(data[2])

if tokens == nil then
  tokens = cap
  last   = now_ms
end

local delta_ms = math.max(0, now_ms - last)
local refill   = (delta_ms / 1000.0) * rate
tokens = math.min(cap, tokens + refill)

local allowed = 0
local retry_after = 0
if tokens >= req then
  tokens = tokens - req
  allowed = 1
else
  local missing = req - tokens
  retry_after = math.ceil((missing / rate) * 1000)
end

redis.call("HMSET", key, "tokens", tokens, "ts", now_ms)
-- ttl roughly one full refill window, keeps memory bounded
redis.call("PEXPIRE", key, math.ceil((cap / rate) * 1000 * 2))

return { allowed, math.floor(tokens), retry_after }
`

// RateLimitConfig defines bucket capacity and refill rate. Capacity = burst
// size; Rate = sustained tokens per second.
type RateLimitConfig struct {
	Capacity int
	Rate     float64
	// KeyFunc derives the bucket key from the request. If nil, defaults to
	// client IP — fine as a baseline but typically replaced with workspace id
	// for authenticated routes.
	KeyFunc func(c *gin.Context) string
}

// RateLimit returns a Gin middleware that enforces the token bucket against
// the supplied Redis client. Failed Redis calls fail open (allow the request)
// so a Redis outage does not take down the API.
func RateLimit(rdb *redis.Client, cfg RateLimitConfig) gin.HandlerFunc {
	script := redis.NewScript(tokenBucketScript)

	keyFunc := cfg.KeyFunc
	if keyFunc == nil {
		keyFunc = func(c *gin.Context) string { return "rl:ip:" + c.ClientIP() }
	}

	return func(c *gin.Context) {
		ctx := c.Request.Context()
		key := keyFunc(c)
		now := nowUnixMs()

		raw, err := script.Run(ctx, rdb,
			[]string{key},
			cfg.Capacity, cfg.Rate, now, 1,
		).Result()
		if err != nil {
			// Fail open. Don't punish users for our Redis problems.
			c.Next()
			return
		}

		result, ok := raw.([]any)
		if !ok || len(result) != 3 {
			c.Next()
			return
		}

		allowed, _ := result[0].(int64)
		remaining, _ := result[1].(int64)
		retryAfterMs, _ := result[2].(int64)

		c.Writer.Header().Set("X-RateLimit-Limit", strconv.Itoa(cfg.Capacity))
		c.Writer.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(remaining, 10))

		if allowed == 0 {
			retrySec := (retryAfterMs + 999) / 1000
			c.Writer.Header().Set("Retry-After", strconv.FormatInt(retrySec, 10))
			response.Error(c, http.StatusTooManyRequests, response.CodeRateLimited,
				fmt.Sprintf("rate limit exceeded, retry in %dms", retryAfterMs))
			return
		}
		c.Next()
	}
}

// nowUnixMs returns current time in milliseconds. Split out so tests can stub.
var nowUnixMs = func() int64 {
	return timeNowMs()
}
