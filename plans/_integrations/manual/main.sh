#!/usr/bin/env bash

echo "simple test"
tc qdisc add dev eth0 root netem delay 200ms
ping www.google.fr -c 3

echo "set 2s latency"
tc qdisc change dev eth0 root netem delay 2000ms
ping www.google.fr -c 3

echo "async test"
echo "reset 200ms latency"
tc qdisc change dev eth0 root netem delay 200ms

ping -c 10 www.google.fr &

# run "echo hello" after 5 seconds
(sleep 5; tc qdisc change dev eth0 root netem delay 2000ms; echo "updated with 2s latency") &

# wait for both commands to complete
wait
