package main

import (
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/libp2p/go-libp2p/p2p/protocol/ping"

	"context"
	"fmt"

	"github.com/testground/sdk-go/network"
	"github.com/testground/sdk-go/run"
	"github.com/testground/sdk-go/runtime"
	"github.com/testground/sdk-go/sync"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/config"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	noise "github.com/libp2p/go-libp2p/p2p/security/noise"
	tls "github.com/libp2p/go-libp2p/p2p/security/tls"
)

var testcases = map[string]interface{}{
	"issue-1488-latency-not-working-correctly": run.InitializedTestCaseFn(runPing), // we don't need the type conversion, but it's here for instructional purposes.
}

func main() {
	run.InvokeMap(testcases)
}

func runPing(runenv *runtime.RunEnv, initCtx *run.InitContext) error {
	runenv.RecordMessage("started test instance, waiting for a minute...")
	// time.Sleep(1 * time.Minute)
	
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	initCtx.MustWaitAllInstancesInitialized(ctx)
	ip := initCtx.NetClient.MustGetDataNetworkIP()

	listenAddr := fmt.Sprintf("/ip4/%s/tcp/0", ip)
	host, err := NewLibp2(ctx,
		"noise",
		libp2p.ListenAddrStrings(listenAddr),
	)

	if err != nil {
		return fmt.Errorf("failed to instantiate libp2p instance: %w", err)
	}
	defer host.Close()

	// Now we instantiate the ping service.
	ping := ping.NewPingService(host)

	// Record our listen addrs.
	runenv.RecordMessage("my listen addrs: %v", host.Addrs())

	// Obtain our own address info, and use the sync service to publish it to a
	// 'peersTopic' topic, where others will read from.
	var (
		hostId = host.ID()
		ai     = &PeerAddrInfo{ID: hostId, Addrs: host.Addrs()}

		// the peers topic where all instances will advertise their AddrInfo.
		peersTopic = sync.NewTopic("peers", new(PeerAddrInfo))

		// initialize a slice to store the AddrInfos of all other peers in the run.
		peers = make([]*PeerAddrInfo, 0, runenv.TestInstanceCount)
	)

	// Publish our own.
	initCtx.SyncClient.MustPublish(ctx, peersTopic, ai)

	// Now subscribe to the peers topic and consume all addresses, storing them
	// in the peers slice.
	peersCh := make(chan *PeerAddrInfo)
	sctx, scancel := context.WithCancel(ctx)
	sub := initCtx.SyncClient.MustSubscribe(sctx, peersTopic, peersCh)

	// Receive the expected number of AddrInfos.
	for len(peers) < cap(peers) {
		select {
		case ai := <-peersCh:
			peers = append(peers, ai)
		case err := <-sub.Done():
			return err
		}
	}
	scancel() // cancels the Subscription.

	// This is a closure that pings all peers in the test in parallel
	pingPeers := func(expectedLatency time.Duration, tag string) error {
		g, gctx := errgroup.WithContext(ctx)
		for _, ai := range peers {
			if ai.ID == hostId {
				continue
			}

			id := ai.ID // capture the ID locally for safe use within the closure.

			g.Go(func() error {
				// a context for the continuous stream of pings.
				pctx, cancel := context.WithCancel(gctx)
				defer cancel()

				// ping 5 times and record the RTT
				rtts := make([]time.Duration, 5)
				pings := ping.Ping(pctx, id)

				for i := 0; i < 5; i++ {
					res := <-pings
					if res.Error != nil {
						return res.Error
					}

					rtts[i] = res.RTT
				}

				// summary
				if (expectedLatency > 0) {
					return verifyLatency(runenv, rtts, expectedLatency * 2 - expectedLatency / 5, expectedLatency * 2 + expectedLatency / 5)
				}
				return nil
			})
		}
		return g.Wait()
	}

	// ☎️  Connect to all other peers.
	for _, ai := range peers {
		if ai.ID == hostId {
			break
		}
		runenv.RecordMessage("Dial peer: %s", ai.ID)
		if err := host.Connect(ctx, *ai); err != nil {
			return err
		}
	}

	runenv.RecordMessage("done dialling my peers")

	// Wait for all peers to signal that they're done with the connection phase.
	initCtx.SyncClient.MustSignalAndWait(ctx, "connected", runenv.TestInstanceCount)

	// 📡  Let's ping all our peers without any traffic shaping rules.
	if err := pingPeers(-1,"initial"); err != nil {
		return err
	}

	// 🕐  Wait for all peers to have finished the initial round.
	initCtx.SyncClient.MustSignalAndWait(ctx, "initial", runenv.TestInstanceCount)
	time.Sleep(1 * time.Second)

	latencies := []time.Duration{
		200 * time.Millisecond,
		25 * time.Millisecond,
		// 50 * time.Millisecond,
		// 100 * time.Millisecond,
		// 200 * time.Millisecond,
	}

	iteration := 0

	for _, latency := range latencies {
		iteration += 1
		runenv.RecordMessage("⚡️  ITERATION ROUND %d", iteration)
		runenv.RecordMessage("(round %d) my latency: %s", iteration, latency)

		// reconfigure our network.
		initCtx.NetClient.MustConfigureNetwork(ctx, &network.Config{
			Network:        "default",
			Enable:         true,
			Default:        network.LinkShape{Latency: latency},
			CallbackState:  sync.State(fmt.Sprintf("network-configured-%d", iteration)),
			CallbackTarget: runenv.TestInstanceCount,
		})
		runenv.RecordMessage("(round %d) Done Updating Network")
		// time.Sleep(1 * time.Second)

		if err := pingPeers(latency, fmt.Sprintf("iteration-%d", iteration)); err != nil {
			return err
		}

		// Signal that we're done with this round and wait for others to be
		// done.
		doneState := sync.State(fmt.Sprintf("done-%d", iteration))
		initCtx.SyncClient.MustSignalAndWait(ctx, doneState, runenv.TestInstanceCount)
	}

	_ = host.Close()
	return nil
}

type PeerAddrInfo = peer.AddrInfo

func NewLibp2(ctx context.Context, secureChannel string, opts ...config.Option) (host.Host, error) {
	security := getSecurityByName(secureChannel)

	return libp2p.New(
		append(opts, security)...,
	)
}

func getSecurityByName(secureChannel string) libp2p.Option {
	switch secureChannel {
	case "noise":
		return libp2p.Security(noise.ID, noise.New)
	case "tls":
		return libp2p.Security(tls.ID, tls.New)
	}
	panic(fmt.Sprintf("unknown secure channel: %s", secureChannel))
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
