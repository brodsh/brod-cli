package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Client is the read-only handle to a customer's Kafka cluster. It wraps a
// franz-go *kgo.Client purely so kadm (the Admin API) can issue Admin / Describe
// / Metadata / ListOffsets / OffsetFetch requests over it.
//
// PILLAR (read-only by design) + PILLAR (metadata only): this type exposes ONLY
// read methods (see reads.go). It never produces and — critically — it never
// consumes: there is no record-fetch/poll path here. guard_test.go fails the
// build if a consume entrypoint ever appears. The underlying *kgo.Client is
// constructed without any consumer options (no topics, no group) and is used
// solely as the transport kadm.NewClient requires.
type Client struct {
	kgo *kgo.Client
	adm *kadm.Client
}

// SASLMechanism is the supported SASL auth mechanisms. Empty means no SASL.
type SASLMechanism string

const (
	SASLNone       SASLMechanism = ""
	SASLPlain      SASLMechanism = "plain"
	SASLScramSHA256 SASLMechanism = "scram-sha-256"
	SASLScramSHA512 SASLMechanism = "scram-sha-512"
)

// SASLConfig holds optional SASL credentials. User/Pass fall back to the
// BROD_KAFKA_USER / BROD_KAFKA_PASS env vars when left empty (so secrets need
// not appear on the command line or in shell history).
type SASLConfig struct {
	Mechanism SASLMechanism
	User      string
	Pass      string
}

// Config describes how to reach the cluster. Bootstrap is required; everything
// else is optional. DialTimeout defaults to 10s when zero.
type Config struct {
	Bootstrap   []string
	TLS         bool
	SASL        SASLConfig
	DialTimeout time.Duration
}

const defaultDialTimeout = 10 * time.Second

// envUser / envPass are the credential env vars (documented in CLAUDE/A-1).
const (
	envUser = "BROD_KAFKA_USER"
	envPass = "BROD_KAFKA_PASS"
)

// Connect opens a read-only admin connection to the cluster. It performs a
// metadata round-trip so authentication / reachability failures surface here
// (with the credentials never echoed back in the error).
func Connect(ctx context.Context, cfg Config) (*Client, error) {
	if len(cfg.Bootstrap) == 0 {
		return nil, errors.New("kafka: no bootstrap servers given")
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Bootstrap...),
		kgo.ClientID("brod"),
	}
	if dt := cfg.DialTimeout; dt > 0 {
		opts = append(opts, kgo.DialTimeout(dt))
	} else {
		opts = append(opts, kgo.DialTimeout(defaultDialTimeout))
	}
	if cfg.TLS {
		// Default system roots; franz-go upgrades the dialer to TLS.
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}

	mech, err := saslMechanism(cfg.SASL)
	if err != nil {
		return nil, err
	}
	if mech != nil {
		opts = append(opts, kgo.SASL(mech))
	}

	kcl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: building client: %w", err)
	}

	c := &Client{kgo: kcl, adm: kadm.NewClient(kcl)}

	// Eagerly verify reachability + auth with a cheap metadata read so the
	// caller gets a clear error before any collection work begins.
	if _, err := c.adm.BrokerMetadata(ctx); err != nil {
		kcl.Close()
		return nil, fmt.Errorf("kafka: connecting to cluster: %w", redactCreds(err, cfg.SASL))
	}
	return c, nil
}

// Close releases the underlying connection.
func (c *Client) Close() {
	if c != nil && c.kgo != nil {
		c.kgo.Close()
	}
}

// saslMechanism builds the franz-go SASL mechanism from the config, resolving
// credentials from the environment when the flags are empty.
func saslMechanism(s SASLConfig) (sasl.Mechanism, error) {
	if s.Mechanism == SASLNone {
		return nil, nil
	}
	user := s.User
	if user == "" {
		user = os.Getenv(envUser)
	}
	pass := s.Pass
	if pass == "" {
		pass = os.Getenv(envPass)
	}
	if user == "" || pass == "" {
		return nil, fmt.Errorf("kafka: SASL %s requires a user and pass (flags or %s/%s env)", s.Mechanism, envUser, envPass)
	}
	switch s.Mechanism {
	case SASLPlain:
		return plain.Auth{User: user, Pass: pass}.AsMechanism(), nil
	case SASLScramSHA256:
		return scram.Auth{User: user, Pass: pass}.AsSha256Mechanism(), nil
	case SASLScramSHA512:
		return scram.Auth{User: user, Pass: pass}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("kafka: unsupported SASL mechanism %q (want plain|scram-sha-256|scram-sha-512)", s.Mechanism)
	}
}

// redactCreds defensively scrubs any credential substrings from an error before
// it can reach a log or the user's terminal. PILLAR-adjacent: never print
// credentials. franz-go errors don't include the password today, but this keeps
// that true even if a future version changes.
func redactCreds(err error, s SASLConfig) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	for _, secret := range []string{s.Pass, os.Getenv(envPass)} {
		if secret != "" {
			msg = strings.ReplaceAll(msg, secret, "***")
		}
	}
	return errors.New(msg)
}
