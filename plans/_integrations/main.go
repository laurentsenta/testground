package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/testground/sdk-go/network"
	"github.com/testground/sdk-go/run"
	"github.com/testground/sdk-go/runtime"
	"github.com/testground/sdk-go/sync"
	"golang.org/x/sync/errgroup"
)

var testcases = map[string]interface{}{
	"issue-1349-silent-failure":                silentFailure,
	"issue-1493-success":                       run.InitializedTestCaseFn(success),
	"issue-1493-optional-failure":              run.InitializedTestCaseFn(optionalFailure),
	"issue-1488-latency-not-working-correctly": run.InitializedTestCaseFn(verifyRTT),
}

func main() {
	run.InvokeMap(testcases)
}

func silentFailure(runenv *runtime.RunEnv) error {
	runenv.RecordMessage("This fails by NOT returning an error and NOT sending a test success status.")
	return nil
}

func success(runenv *runtime.RunEnv, initCtx *run.InitContext) error {
	runenv.RecordMessage("success!")
	return nil
}

func optionalFailure(runenv *runtime.RunEnv, initCtx *run.InitContext) error {
	shouldFail := runenv.BooleanParam("should_fail")
	runenv.RecordMessage("Test run with shouldFail: %s", shouldFail)

	if shouldFail {
		return errors.New("failing as requested")
	}

	return nil
}

func verifyRTT(runenv *runtime.RunEnv, initCtx *run.InitContext) error {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	client := initCtx.SyncClient
	netclient := initCtx.NetClient

	// Wait until all instances in this test run have signalled.
	initCtx.MustWaitAllInstancesInitialized(ctx)

	// Find my IP address
	myIp, err := netclient.GetDataNetworkIP()
	if err != nil {
		return err
	}

	// Start a server
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{Port: 1234})
	if err != nil {
		return err
	}
	defer listener.Close()
	go server(runenv, listener)

	// Exhange IPs with the other instances
	peersTopic := sync.NewTopic("peers", new(net.IP))
	_ = client.MustPublish(ctx, peersTopic, myIp)

	peersCh := make(chan net.IP)
	peers := make([]net.IP, 0, runenv.TestInstanceCount)
	sub := initCtx.SyncClient.MustSubscribe(ctx, peersTopic, peersCh)

	// Wait for the other instance to publish their IP address
	for len(peers) < runenv.TestInstanceCount {
		select {
		case err := <-sub.Done():
			return err
		case ip := <-peersCh:
			peers = append(peers, ip)
		}
	}

	clientReadyState := sync.State("client-ready")
	clientDoneState := sync.State("client-done")

	// Setting up the client
	client.MustSignalAndWait(ctx, clientReadyState, runenv.TestInstanceCount)

	// Connect to the other peers and keep the conn in a map
	conns, err := prepareConns(runenv, peers, myIp)
	defer clearConns(conns)
	if err != nil {
		return err
	}

	// Configure the network
	latencies := []time.Duration{
		200 * time.Millisecond,
		25 * time.Millisecond,
		// 50 * time.Millisecond,
		// 100 * time.Millisecond,
		// 200 * time.Millisecond,
	}

	for _, latency := range latencies {
		expectedRTT := latency * 2

		runenv.RecordMessage("RTT: %s", expectedRTT)

		config := &network.Config{
			Network: "default",
			Enable:  true,
			Default: network.LinkShape{
				Latency: latency,
			},
			CallbackState:  sync.State("network-configured"),
			CallbackTarget: runenv.TestInstanceCount,
		}

		// conns, err := prepareConns(runenv, peers, myIp)

		// Wait for the network to be configured
		netclient.MustConfigureNetwork(ctx, config)

		// conns, err := prepareConns(runenv, peers, myIp)
		time.Sleep(500 * time.Millisecond)

		// ping pong with the peers
		err = pingPeers(ctx, runenv, conns, expectedRTT-expectedRTT/5, expectedRTT+expectedRTT/5)
		if err != nil {
			return err
		}

		// Done with that run (will iterate later)
		client.MustSignalAndWait(ctx, clientDoneState, runenv.TestInstanceCount)
		// clearConns(conns)
	}

	return nil
}

func prepareConns(runenv *runtime.RunEnv, peers []net.IP, myIp net.IP) (map[string]net.Conn, error) {
	conns := make(map[string]net.Conn)

	for _, peer := range peers {
		if peer.Equal(myIp) {
			continue
		}

		runenv.RecordMessage("Attempting to connect to %s", peer)
		// conn, err := net.DialUDP("udp", nil, &net.UDPAddr{
		// 	IP:   peer,
		// 	Port: 1234,
		// })

		conn, err := net.DialTCP("tcp4", nil, &net.TCPAddr{
			IP:   peer,
			Port: 1234,
		})

		if err != nil {
			return nil, err
		}

		// disable Nagle's algorithm to measure latency.
		err = conn.SetNoDelay(true)
		if err != nil {
			return nil, err
		}

		conns[peer.String()] = conn
	}

	return conns, nil
}

func clearConns(conns map[string]net.Conn) {
	for _, conn := range conns {
		conn.Close()
	}
}

func pingPeers(ctx context.Context, runenv *runtime.RunEnv, conns map[string]net.Conn, minRTT time.Duration, maxRTT time.Duration) error {
	g, _ := errgroup.WithContext(ctx)

	for _, conn := range conns {
		conn := conn

		g.Go(func() error {
			deltas, err := pingPeer(conn)
			if err != nil {
				return err
			}

			return verifyLatency(runenv, deltas, minRTT, maxRTT)
		})
	}

	return g.Wait()
}

func pingPeer(conn net.Conn) ([]time.Duration, error) {
	buf := make([]byte, 1)
	deltas := make([]time.Duration, 0, 3)

	// Send N pings
	for i := 0; i < 4; i++ {
		start := time.Now()

		// Send the ping
		conn.Write([]byte{byte(i)})

		// Receive the pong
		_, err := conn.Read(buf)
		if err != nil {
			return nil, err
		}

		end := time.Now()

		// append to the array of deltas
		deltas = append(deltas, end.Sub(start))
	}

	return deltas, nil
}

func verifyLatency(runenv *runtime.RunEnv, deltas []time.Duration, minRTT time.Duration, maxRTT time.Duration) error {
	min, max, avg := Summarize(deltas)
	runenv.RecordMessage("RTT %v - min: %v, max: %v, avg: %v", deltas, min, max, avg)

	if max > maxRTT {
		return fmt.Errorf("max RTT is invalid: %v > %v", max, maxRTT)
	}
	if min < minRTT {
		return fmt.Errorf("min RTT is invalid: %v < %v", min, minRTT)
	}

	return nil
}

func handleConnection(conn *net.TCPConn) {
	defer conn.Close()
	// disable Nagle's algorithm to measure latency.
	err := conn.SetNoDelay(true)
	if err != nil {
		panic(err)
	}

	buf := make([]byte, 1)
	for {
		_, err := conn.Read(buf)
		if err == io.EOF {
			return
		}
		if err != nil {
			panic(err)
		}
		conn.Write(buf)
	}
}

func server(runenv *runtime.RunEnv, listener *net.TCPListener) {
	for {
		runenv.RecordMessage("accepting new connections")
		conn, err := listener.AcceptTCP()
		if err != nil {
			return
		}
		go handleConnection(conn)
	}
}

func Summarize(deltas []time.Duration) (min, max, avg time.Duration) {
	min = deltas[0]
	max = deltas[0]
	avg = deltas[0]

	for _, d := range deltas[1:] {
		if d < min {
			min = d
		}
		if d > max {
			max = d
		}
		avg += d
	}

	avg /= time.Duration(len(deltas))
	return
}
