package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/steveyegge/beads/internal/storage/dbproxy/util"
)

type ExternalDoltConfig struct {
	Host string
	Port int
	Socket string
	TLSRequired bool
	TLSCert string
	TLSKey string
	KeepAlivePeriod time.Duration
}

func (c ExternalDoltConfig) Validate() error {
	hasHost := c.Host != ""
	hasPort := c.Port != 0
	hasSocket := c.Socket != ""

	switch {
	case hasSocket && (hasHost || hasPort):
		return errors.New("ExternalDoltConfig: set either Socket OR (Host, Port), not both")
	case !hasSocket && !hasHost && !hasPort:
		return errors.New("ExternalDoltConfig: must set Socket or (Host, Port)")
	case hasHost && !hasPort:
		return errors.New("ExternalDoltConfig: Host requires Port")
	case !hasHost && hasPort:
		return errors.New("ExternalDoltConfig: Port requires Host")
	}

	if hasHost && (c.Port < 1 || c.Port > 65535) {
		return fmt.Errorf("ExternalDoltConfig: Port %d out of range [1, 65535]", c.Port)
	}

	if hasSocket && !filepath.IsAbs(c.Socket) {
		return fmt.Errorf("ExternalDoltConfig: Socket %q is not absolute", c.Socket)
	}

	switch {
	case c.TLSCert != "" && c.TLSKey == "":
		return errors.New("ExternalDoltConfig: TLSCert set without TLSKey")
	case c.TLSCert == "" && c.TLSKey != "":
		return errors.New("ExternalDoltConfig: TLSKey set without TLSCert")
	}

	if c.TLSCert != "" && !filepath.IsAbs(c.TLSCert) {
		return fmt.Errorf("ExternalDoltConfig: TLSCert %q is not absolute", c.TLSCert)
	}
	if c.TLSKey != "" && !filepath.IsAbs(c.TLSKey) {
		return fmt.Errorf("ExternalDoltConfig: TLSKey %q is not absolute", c.TLSKey)
	}

	if c.KeepAlivePeriod < 0 {
		return fmt.Errorf("ExternalDoltConfig: KeepAlivePeriod %s is negative", c.KeepAlivePeriod)
	}

	return nil
}

type ExternalDoltServer struct {
	id              string
	host            string
	port            int
	socket          string
	tlsRequired     bool
	tlsCert         string
	tlsKey          string
	keepAlivePeriod time.Duration

	started atomic.Bool
}

var _ DatabaseServer = (*ExternalDoltServer)(nil)

func NewExternalDoltServer(cfg ExternalDoltConfig) (*ExternalDoltServer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	keepAlive := cfg.KeepAlivePeriod
	if keepAlive == 0 {
		keepAlive = defaultKeepAlivePeriod
	}
	sum := sha256.Sum256([]byte(externalDoltServerTarget(cfg)))
	return &ExternalDoltServer{
		id:              hex.EncodeToString(sum[:]),
		host:            cfg.Host,
		port:            cfg.Port,
		socket:          cfg.Socket,
		tlsRequired:     cfg.TLSRequired,
		tlsCert:         cfg.TLSCert,
		tlsKey:          cfg.TLSKey,
		keepAlivePeriod: keepAlive,
	}, nil
}

func externalDoltServerTarget(cfg ExternalDoltConfig) string {
	if cfg.Socket != "" {
		return "unix://" + cfg.Socket
	}
	return "tcp://" + net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
}

func (s *ExternalDoltServer) ID(_ context.Context) string {
	return s.id
}

func (s *ExternalDoltServer) DSN(_ context.Context, database, user, password string) string {
	dsn := util.DoltServerDSN{
		User:        user,
		Password:    password,
		Database:    database,
		TLSRequired: s.tlsRequired,
		TLSCert:     s.tlsCert,
		TLSKey:      s.tlsKey,
	}
	if s.socket != "" {
		dsn.Socket = s.socket
	} else {
		dsn.Host = s.host
		dsn.Port = s.port
	}
	return dsn.String()
}

func (s *ExternalDoltServer) Start(_ context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("server: ExternalDoltServer.Start: server already started")
	}
	return nil
}

func (s *ExternalDoltServer) Stop(_ context.Context) error {
	s.started.Store(false)
	return nil
}

func (s *ExternalDoltServer) Running(_ context.Context) bool {
	return s.started.Load()
}

func (s *ExternalDoltServer) Dial(ctx context.Context) (net.Conn, error) {
	network, addr := "tcp", net.JoinHostPort(s.host, strconv.Itoa(s.port))
	if s.socket != "" {
		network, addr = "unix", s.socket
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, network, addr)
	if err != nil {
		return nil, fmt.Errorf("server: ExternalDoltServer.Dial: %w", err)
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(s.keepAlivePeriod)
	}
	return conn, nil
}
