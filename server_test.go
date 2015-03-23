package manners

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

type httpInterface interface {
	ListenAndServe() error
	ListenAndServeTLS(certFile, keyFile string) error
	Serve(listener net.Listener) error
}

// an inefficient replica of a waitgroup that can be introspected
type testWg struct {
	sync.Mutex
	count        int
	waitCalled   chan int
	countChanged chan int
}

func newTestWg() *testWg {
	return &testWg{
		waitCalled:   make(chan int, 1),
		countChanged: make(chan int, 1024),
	}
}

func (wg *testWg) Add(delta int) {
	wg.Lock()
	wg.count++
	wg.countChanged <- wg.count
	wg.Unlock()
}

func (wg *testWg) Done() {
	wg.Lock()
	wg.count--
	wg.countChanged <- wg.count
	wg.Unlock()
}

func (wg *testWg) Wait() {
	wg.Lock()
	wg.waitCalled <- wg.count
	wg.Unlock()
}

// a simple step-controllable http client
type client struct {
	tls         bool
	addr        net.Addr
	connected   chan error
	sendrequest chan bool
	response    chan *rawResponse
	closed      chan bool
}

type rawResponse struct {
	body []string
	err  error
}

func (c *client) Run() {
	go func() {
		var err error
		conn, err := net.Dial(c.addr.Network(), c.addr.String())
		if err != nil {
			c.connected <- err
			return
		}
		if c.tls {
			conn = tls.Client(conn, &tls.Config{InsecureSkipVerify: true})
		}
		c.connected <- nil
		for <-c.sendrequest {
			_, err = conn.Write([]byte("GET / HTTP/1.1\nHost: localhost:8000\n\n"))
			if err != nil {
				c.response <- &rawResponse{err: err}
			}
			// Read response; no content
			scanner := bufio.NewScanner(conn)
			var lines []string
			for scanner.Scan() {
				// our null handler doesn't send a body, so we know the request is
				// done when we reach the blank line after the headers
				line := scanner.Text()
				if line == "" {
					break
				}
				lines = append(lines, line)
			}
			c.response <- &rawResponse{lines, scanner.Err()}
		}
		conn.Close()
		ioutil.ReadAll(conn)
		c.closed <- true
	}()
}

func newClient(addr net.Addr, tls bool) *client {
	return &client{
		addr:        addr,
		tls:         tls,
		connected:   make(chan error),
		sendrequest: make(chan bool),
		response:    make(chan *rawResponse),
		closed:      make(chan bool),
	}
}

// a handler that returns 200 ok with no body
var nullHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})

func startGenericServer(t *testing.T, server *GracefulServer, statechanged chan http.ConnState, runner func() error) (l net.Listener, errc chan error) {
	server.Addr = "localhost:0"
	server.Handler = nullHandler
	if statechanged != nil {
		// Wrap the ConnState handler with something that will notify
		// the statechanged channel when a state change happens
		server.ConnState = func(conn net.Conn, newState http.ConnState) {
			gconn := conn.LocalAddr().(*gracefulAddr).gconn
			s := gconn.lastHTTPState
			statechanged <- s
		}
	}

	//server.up = make(chan chan bool))
	server.up = make(chan net.Listener)
	exitchan := make(chan error)

	go func() {
		err := runner()
		if err != nil {
			exitchan <- err
		} else {
			exitchan <- nil
		}
	}()

	// wait for server socket to be bound
	select {
	case l = <-server.up:
		// all good

	case err := <-exitchan:
		// all bad
		t.Fatal("Server failed to start", err)
	}
	return l, exitchan
}

func startServer(t *testing.T, server *GracefulServer, statechanged chan http.ConnState) (l net.Listener, errc chan error) {
	runner := func() error {
		return server.ListenAndServe()
	}

	return startGenericServer(t, server, statechanged, runner)
}

func startTLSServer(t *testing.T, server *GracefulServer, certFile, keyFile string, statechanged chan http.ConnState) (l net.Listener, errc chan error) {
	runner := func() error {
		return server.ListenAndServeTLS(certFile, keyFile)
	}

	return startGenericServer(t, server, statechanged, runner)
}

// Test that the method signatures of the methods we override from net/http/Server match those of the original.
func TestInterface(t *testing.T) {
	var original, ours interface{}
	original = &http.Server{}
	ours = &GracefulServer{}
	if _, ok := original.(httpInterface); !ok {
		t.Errorf("httpInterface definition does not match the canonical server!")
	}
	if _, ok := ours.(httpInterface); !ok {
		t.Errorf("GracefulServer does not implement httpInterface")
	}
}

// Tests that the server allows in-flight requests to complete before shutting down.
func TestGracefulness(t *testing.T) {
	server := NewServer()
	stateChangedCh := make(chan http.ConnState)
	wg := newTestWg()
	server.wg = wg
	listener, exitchan := startServer(t, server, stateChangedCh)

	client := newClient(listener.Addr(), false)
	client.Run()

	// wait for client to connect, but don't let it send the request yet
	if err := <-client.connected; err != nil {
		t.Fatal("Client failed to connect to server", err)
	}
	// Even though the client is connected, the server ConnState handler may
	// not know about that yet. So wait until it is called.
	waitForState(t, stateChangedCh, http.StateNew, "Request not received")

	server.Close()

	waiting := <-wg.waitCalled
	if waiting < 1 {
		t.Errorf("Expected the waitgroup to equal 1 at shutdown; actually %d", waiting)
	}

	// allow the client to finish sending the request and make sure the server exits after
	// (client will be in connected but idle state at that point)
	client.sendrequest <- true
	close(client.sendrequest)
	if err := <-exitchan; err != nil {
		t.Error("Unexpected error during shutdown", err)
	}
}

var stateTests = []struct {
	states       []http.ConnState
	finalWgCount int
}{
	{[]http.ConnState{http.StateNew, http.StateActive}, 1},
	{[]http.ConnState{http.StateNew, http.StateClosed}, 0},
	{[]http.ConnState{http.StateNew, http.StateActive, http.StateClosed}, 0},
	{[]http.ConnState{http.StateNew, http.StateActive, http.StateHijacked}, 0},
	{[]http.ConnState{http.StateNew, http.StateActive, http.StateIdle}, 0},
	{[]http.ConnState{http.StateNew, http.StateActive, http.StateIdle, http.StateActive}, 1},
	{[]http.ConnState{http.StateNew, http.StateActive, http.StateIdle, http.StateActive, http.StateIdle}, 0},
	{[]http.ConnState{http.StateNew, http.StateActive, http.StateIdle, http.StateActive, http.StateClosed}, 0},
	{[]http.ConnState{http.StateNew, http.StateActive, http.StateIdle, http.StateActive, http.StateIdle, http.StateClosed}, 0},
}

func fmtstates(states []http.ConnState) string {
	names := make([]string, len(states))
	for i, s := range states {
		names[i] = s.String()
	}
	return strings.Join(names, " -> ")
}

// Test the state machine in isolation without a network connection
func TestStateTransitions(t *testing.T) {
	for _, test := range stateTests {
		fmt.Println("Starting test ", fmtstates(test.states))
		server := NewServer()
		wg := newTestWg()
		server.wg = wg
		startServer(t, server, nil)

		conn := &gracefulConn{Conn: &fakeConn{}}
		for _, newState := range test.states {
			server.ConnState(conn, newState)
		}

		server.Close()
		waiting := <-wg.waitCalled
		if waiting != test.finalWgCount {
			t.Errorf("%s - Waitcount should be %d, got %d", fmtstates(test.states), test.finalWgCount, waiting)
		}

	}
}

type fakeConn struct {
	net.Conn
	closeCalled bool
	localAddr   net.Addr
}

func (f *fakeConn) LocalAddr() net.Addr {
	return &net.IPAddr{}
}

func (c *fakeConn) Close() error {
	c.closeCalled = true
	return nil
}

type fakeListener struct {
	acceptRelease chan bool
	closeCalled   chan bool
}

func newFakeListener() *fakeListener { return &fakeListener{make(chan bool, 1), make(chan bool, 1)} }

func (l *fakeListener) Addr() net.Addr {
	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	return addr
}

func (l *fakeListener) Close() error {
	l.closeCalled <- true
	l.acceptRelease <- true
	return nil
}

func (l *fakeListener) Accept() (net.Conn, error) {
	<-l.acceptRelease
	return nil, errors.New("connection closed")
}

func waitForState(t *testing.T, waiter chan http.ConnState, state http.ConnState, errmsg string) {
	for {
		select {
		case ns := <-waiter:
			if ns == state {
				return
			}
		case <-time.After(time.Second):
			t.Fatal(errmsg)
		}
	}
}

// Test that a request moving from active->idle->active using an actual network connection still results in a corect shutdown.
func TestStateTransitionActiveIdleActive(t *testing.T) {
	server := NewServer()
	wg := newTestWg()
	statechanged := make(chan http.ConnState)
	server.wg = wg
	listener, exitchan := startServer(t, server, statechanged)

	client := newClient(listener.Addr(), false)
	client.Run()

	// wait for client to connect, but don't let it send the request
	if err := <-client.connected; err != nil {
		t.Fatal("Client failed to connect to server", err)
	}

	for i := 0; i < 2; i++ {
		client.sendrequest <- true
		waitForState(t, statechanged, http.StateActive, "Client failed to reach active state")
		<-client.response
		waitForState(t, statechanged, http.StateIdle, "Client failed to reach idle state")
	}

	// client is now in an idle state

	server.Close()
	waiting := <-wg.waitCalled
	if waiting != 0 {
		t.Errorf("Waitcount should be zero, got %d", waiting)
	}

	if err := <-exitchan; err != nil {
		t.Error("Unexpected error during shutdown", err)
	}
}

// If a request is sent to a closed server via a kept alive connection then
// the server closes the connection upon receiving the request.
func TestRequestAfterClose(t *testing.T) {
	// Given
	server := NewServer()
	srvStateChangedCh := make(chan http.ConnState, 100)
	listener, srvClosedCh := startServer(t, server, srvStateChangedCh)

	client := newClient(listener.Addr(), false)
	client.Run()
	<-client.connected
	client.sendrequest <- true
	<-client.response

	server.Close()
	if err := <-srvClosedCh; err != nil {
		t.Error("Unexpected error during shutdown", err)
	}

	// When
	client.sendrequest <- true
	rr := <-client.response

	// Then
	if rr.body != nil || rr.err != nil {
		t.Errorf("Request should be rejected, body=%v, err=%v", rr.body, rr.err)
	}
}

// Test state transitions from new->active->-idle->closed using an actual
// network connection and make sure the waitgroup count is correct at the end.
func TestStateTransitionActiveIdleClosed(t *testing.T) {
	var (
		listener net.Listener
		exitchan chan error
	)

	keyFile, err1 := NewTempFile(localhostKey)
	certFile, err2 := NewTempFile(localhostCert)
	defer keyFile.Unlink()
	defer certFile.Unlink()

	if err1 != nil || err2 != nil {
		t.Fatal("Failed to create temporary files", err1, err2)
	}

	for _, withTLS := range []bool{false, true} {
		server := NewServer()
		wg := newTestWg()
		statechanged := make(chan http.ConnState)
		server.wg = wg
		if withTLS {
			listener, exitchan = startTLSServer(t, server, certFile.Name(), keyFile.Name(), statechanged)
		} else {
			listener, exitchan = startServer(t, server, statechanged)
		}

		client := newClient(listener.Addr(), withTLS)
		client.Run()

		// wait for client to connect, but don't let it send the request
		if err := <-client.connected; err != nil {
			t.Fatal("Client failed to connect to server", err)
		}

		client.sendrequest <- true
		waitForState(t, statechanged, http.StateActive, "Client failed to reach active state")

		rr := <-client.response
		if rr.err != nil {
			t.Fatalf("tls=%t unexpected error from client %s", withTLS, rr.err)
		}

		waitForState(t, statechanged, http.StateIdle, "Client failed to reach idle state")

		// client is now in an idle state
		close(client.sendrequest)
		<-client.closed
		waitForState(t, statechanged, http.StateClosed, "Client failed to reach closed state")

		server.Close()
		waiting := <-wg.waitCalled
		if waiting != 0 {
			t.Errorf("Waitcount should be zero, got %d", waiting)
		}

		if err := <-exitchan; err != nil {
			t.Error("Unexpected error during shutdown", err)
		}
	}
}

// Test that supplying a non GracefulListener to Serve works
// correctly (ie. that the listener is wrapped to become graceful)
func TestWrapConnection(t *testing.T) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal("Failed to create listener", err)
	}

	s := NewServer()
	s.up = make(chan net.Listener)

	var called bool
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		s.Close() // clean shutdown as soon as handler exits
	})
	s.Handler = handler

	serverr := make(chan error)

	go func() {
		serverr <- s.Serve(l)
	}()

	gl := <-s.up
	if _, ok := gl.(*GracefulListener); !ok {
		t.Fatal("connection was not wrapped into a GracefulListener")
	}

	addr := l.Addr()
	if _, err := http.Get("http://" + addr.String()); err != nil {
		t.Fatal("Get failed", err)
	}

	if err := <-serverr; err != nil {
		t.Fatal("Error from Serve()", err)
	}

	if !called {
		t.Error("Handler was not called")
	}

}

// Tests that the server begins to shut down when told to and does not accept
// new requests once shutdown has begun
func TestShutdown(t *testing.T) {
	server := NewServer()
	stateChangedCh := make(chan http.ConnState)
	wg := newTestWg()
	server.wg = wg
	listener, exitchan := startServer(t, server, stateChangedCh)

	client1 := newClient(listener.Addr(), false)
	client1.Run()

	// wait for client1 to connect
	if err := <-client1.connected; err != nil {
		t.Fatal("Client failed to connect to server", err)
	}
	// Even though the client is connected, the server ConnState handler may
	// not know about that yet. So wait until it is called.
	waitForState(t, stateChangedCh, http.StateNew, "Request not received")

	// start the shutdown; once it hits waitgroup.Wait()
	// the listener should of been closed, though client1 is still connected
	server.Close()

	waiting := <-wg.waitCalled
	if waiting != 1 {
		t.Errorf("Waitcount should be one, got %d", waiting)
	}

	// should get connection refused at this point
	client2 := newClient(listener.Addr(), false)
	client2.Run()

	if err := <-client2.connected; err == nil {
		t.Fatal("client2 connected when it should of received connection refused")
	}

	// let client1 finish so the server can exit
	close(client1.sendrequest) // don't bother sending an actual request

	<-exitchan
}

// Use the top level functions to instantiate servers and make sure
// they all shutdown when Close() is called
func TestGlobalShutdown(t *testing.T) {
	laserr := make(chan error)
	lastlserr := make(chan error)
	serveerr := make(chan error)

	go func() {
		laserr <- ListenAndServe("127.0.0.1:0", nullHandler)
	}()

	go func() {
		keyFile, _ := NewTempFile(localhostKey)
		certFile, _ := NewTempFile(localhostCert)
		defer keyFile.Unlink()
		defer certFile.Unlink()
		lastlserr <- ListenAndServeTLS("127.0.0.1:0", certFile.Name(), keyFile.Name(), nullHandler)
	}()

	go func() {
		l := newFakeListener()
		serveerr <- Serve(l, nullHandler)
	}()

	// wait for registration
	expected := 3
	var sl int
	for sl < expected {
		m.Lock()
		sl = len(servers)
		m.Unlock()
		time.Sleep(time.Millisecond)
	}

	Close()

	for i := 0; i < expected; i++ {
		select {
		case err := <-laserr:
			if err != nil {
				t.Error("ListenAndServe returned error", err)
			}
			laserr = nil

		case err := <-lastlserr:
			if err != nil {
				t.Error("ListenAndServeTLS returned error", err)
			}
			lastlserr = nil

		case err := <-serveerr:
			if err != nil {
				t.Error("Serve returned error", err)
			}
			serveerr = nil
		case <-time.After(time.Second):
			t.Fatal("Timed out waiting for servers to exit")
		}
	}

}

// Hijack listener
func TestHijackListener(t *testing.T) {
	server := NewServer()
	wg := newTestWg()
	server.wg = wg
	listener, exitchan := startServer(t, server, nil)

	client := newClient(listener.Addr(), false)
	client.Run()

	// wait for client to connect, but don't let it send the request yet
	if err := <-client.connected; err != nil {
		t.Fatal("Client failed to connect to server", err)
	}

	// Make sure server1 got the request and added it to the waiting group
	<-wg.countChanged

	wg2 := newTestWg()
	server2, err := server.HijackListener(new(http.Server), nil)
	server2.wg = wg2
	if err != nil {
		t.Fatal("Failed to hijack listener", err)
	}

	listener2, exitchan2 := startServer(t, server2, nil)

	// Close the first server
	server.Close()

	// First server waits for the first request to finish
	waiting := <-wg.waitCalled
	if waiting < 1 {
		t.Errorf("Expected the waitgroup to equal 1 at shutdown; actually %d", waiting)
	}

	// allow the client to finish sending the request and make sure the server exits after
	// (client will be in connected but idle state at that point)
	client.sendrequest <- true
	close(client.sendrequest)
	if err := <-exitchan; err != nil {
		t.Error("Unexpected error during shutdown", err)
	}

	client2 := newClient(listener2.Addr(), false)
	client2.Run()

	// wait for client to connect, but don't let it send the request yet
	select {
	case err := <-client2.connected:
		if err != nil {
			t.Fatal("Client failed to connect to server", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout connecting to the server", err)
	}

	// Close the second server
	server2.Close()

	waiting = <-wg2.waitCalled
	if waiting < 1 {
		t.Errorf("Expected the waitgroup to equal 1 at shutdown; actually %d", waiting)
	}

	// allow the client to finish sending the request and make sure the server exits after
	// (client will be in connected but idle state at that point)
	client2.sendrequest <- true
	// Make sure that request resulted in success
	if rr := <-client2.response; rr.err != nil {
		t.Errorf("Client failed to write the request, error: %s", err)
	}
	close(client2.sendrequest)
	if err := <-exitchan2; err != nil {
		t.Error("Unexpected error during shutdown", err)
	}
}

type tempFile struct {
	*os.File
}

func NewTempFile(content []byte) (*tempFile, error) {
	f, err := ioutil.TempFile("", "graceful-test")
	if err != nil {
		return nil, err
	}

	f.Write(content)
	return &tempFile{f}, nil
}

func (tf *tempFile) Unlink() {
	if tf.File != nil {
		os.Remove(tf.Name())
		tf.File = nil
	}
}

// localhostCert is a PEM-encoded TLS cert with SAN IPs
// "127.0.0.1" and "[::1]", expiring at the last second of 2049 (the end
// of ASN.1 time).
// generated from src/pkg/crypto/tls:
// go run generate_cert.go  --rsa-bits 512 --host 127.0.0.1,::1,example.com --ca --start-date "Jan 1 00:00:00 1970" --duration=1000000h
var localhostCert = []byte(`-----BEGIN CERTIFICATE-----
MIIBdzCCASOgAwIBAgIBADALBgkqhkiG9w0BAQUwEjEQMA4GA1UEChMHQWNtZSBD
bzAeFw03MDAxMDEwMDAwMDBaFw00OTEyMzEyMzU5NTlaMBIxEDAOBgNVBAoTB0Fj
bWUgQ28wWjALBgkqhkiG9w0BAQEDSwAwSAJBAN55NcYKZeInyTuhcCwFMhDHCmwa
IUSdtXdcbItRB/yfXGBhiex00IaLXQnSU+QZPRZWYqeTEbFSgihqi1PUDy8CAwEA
AaNoMGYwDgYDVR0PAQH/BAQDAgCkMBMGA1UdJQQMMAoGCCsGAQUFBwMBMA8GA1Ud
EwEB/wQFMAMBAf8wLgYDVR0RBCcwJYILZXhhbXBsZS5jb22HBH8AAAGHEAAAAAAA
AAAAAAAAAAAAAAEwCwYJKoZIhvcNAQEFA0EAAoQn/ytgqpiLcZu9XKbCJsJcvkgk
Se6AbGXgSlq+ZCEVo0qIwSgeBqmsJxUu7NCSOwVJLYNEBO2DtIxoYVk+MA==
-----END CERTIFICATE-----`)

// localhostKey is the private key for localhostCert.
var localhostKey = []byte(`-----BEGIN RSA PRIVATE KEY-----
MIIBPAIBAAJBAN55NcYKZeInyTuhcCwFMhDHCmwaIUSdtXdcbItRB/yfXGBhiex0
0IaLXQnSU+QZPRZWYqeTEbFSgihqi1PUDy8CAwEAAQJBAQdUx66rfh8sYsgfdcvV
NoafYpnEcB5s4m/vSVe6SU7dCK6eYec9f9wpT353ljhDUHq3EbmE4foNzJngh35d
AekCIQDhRQG5Li0Wj8TM4obOnnXUXf1jRv0UkzE9AHWLG5q3AwIhAPzSjpYUDjVW
MCUXgckTpKCuGwbJk7424Nb8bLzf3kllAiA5mUBgjfr/WtFSJdWcPQ4Zt9KTMNKD
EUO0ukpTwEIl6wIhAMbGqZK3zAAFdq8DD2jPx+UJXnh0rnOkZBzDtJ6/iN69AiEA
1Aq8MJgTaYsDQWyU/hDq5YkDJc9e9DSCvUIzqxQWMQE=
-----END RSA PRIVATE KEY-----`)
