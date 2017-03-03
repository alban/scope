package endpoint

import (
	"strconv"

	log "github.com/Sirupsen/logrus"
	"github.com/weaveworks/scope/probe/endpoint/procspy"
	"github.com/weaveworks/scope/probe/process"
	"github.com/weaveworks/scope/report"
)

// connectionTrackerConfig are the config options for the endpoint tracker.
type connectionTrackerConfig struct {
	HostID       string
	HostName     string
	SpyProcs     bool
	UseConntrack bool
	WalkProc     bool
	UseEbpfConn  bool
	ProcRoot     string
	BufferSize   int
	Scanner      procspy.ConnectionScanner
	DNSSnooper   *DNSSnooper
}

type connectionTracker struct {
	conf            connectionTrackerConfig
	flowWalker      flowWalker // Interface
	ebpfTracker     eventTracker
	reverseResolver *reverseResolver
}

func newConnectionTracker(conf connectionTrackerConfig) connectionTracker {
	if !conf.UseEbpfConn {
		// ebpf OFF, use flowWalker
		return connectionTracker{
			conf:            conf,
			flowWalker:      newConntrackFlowWalker(conf.UseConntrack, conf.ProcRoot, conf.BufferSize, "--any-nat"),
			ebpfTracker:     nil,
			reverseResolver: newReverseResolver(),
		}
	}
	// When ebpf will be active by default, check if it starts correctly otherwise fallback to flowWalk
	et, err := newEbpfTracker(conf.UseEbpfConn)
	if err != nil {
		// TODO: fallback to flowWalker, when ebpf is enabled by default
		log.Errorf("Error setting up the ebpfTracker, connections will not be reported: %s", err)
		noopConnectionTracker := connectionTracker{
			conf:            conf,
			flowWalker:      nil,
			ebpfTracker:     nil,
			reverseResolver: nil,
		}
		return noopConnectionTracker
	}
	// Run conntrack and proc parsing synchronously only once to initialize ebpfTracker
	seenTuples := map[string]fourTuple{}
	// Consult the flowWalker to get the initial state
	if !conf.UseConntrack {
		log.Warn("conntrack is disabled")
	} else if err = IsConntrackSupported(conf.ProcRoot); err != nil {
		log.Warnf("Not using conntrack: not supported by the kernel: %s", err)
	} else {
		existingFlows, err := existingConnections([]string{"--any-nat"})
		if err != nil {
			log.Errorf("conntrack existingConnections error: %v", err)
		} else {
			for _, f := range existingFlows {
				tuple := fourTuple{
					f.Original.Layer3.SrcIP,
					f.Original.Layer3.DstIP,
					uint16(f.Original.Layer4.SrcPort),
					uint16(f.Original.Layer4.DstPort),
				}
				// Handle DNAT-ed connections in the initial state
				if f.Original.Layer3.DstIP != f.Reply.Layer3.SrcIP {
					tuple = fourTuple{
						f.Reply.Layer3.DstIP,
						f.Reply.Layer3.SrcIP,
						uint16(f.Reply.Layer4.DstPort),
						uint16(f.Reply.Layer4.SrcPort),
					}
				}
				seenTuples[tuple.key()] = tuple
			}
		}
	}

	var processCache *process.CachingWalker
	var scanner procspy.ConnectionScanner
	processCache = process.NewCachingWalker(process.NewWalker(conf.ProcRoot))
	processCache.Tick()
	scanner = procspy.NewSyncConnectionScanner(processCache)
	defer scanner.Stop()
	conns, err := scanner.Connections(conf.SpyProcs)
	if err != nil {
		log.Errorf("Error initializing ebpfTracker while scanning /proc, continue without initial connections: %s", err)
		et.feedInitialConnectionsEmpty()
	} else {
		et.feedInitialConnections(conns, seenTuples, report.MakeHostNodeID(conf.HostID))
	}
	ct := connectionTracker{
		conf:            conf,
		flowWalker:      nil,
		ebpfTracker:     et,
		reverseResolver: newReverseResolver(),
	}
	return ct
}

// ReportConnections calls trackers accordingly to the configuration.
// When ebpf is enabled, only performEbpfTrack() is called
func (t *connectionTracker) ReportConnections(rpt *report.Report) {
	hostNodeID := report.MakeHostNodeID(t.conf.HostID)

	if t.ebpfTracker != nil {
		t.performEbpfTrack(rpt, hostNodeID)
		return
	}

	// seenTuples contains information about connections seen by conntrack and it will be passed to the /proc parser
	seenTuples := map[string]fourTuple{}
	if t.flowWalker != nil {
		t.performFlowWalk(rpt, &seenTuples)
	}
	if t.conf.WalkProc {
		t.performWalkProc(rpt, hostNodeID, &seenTuples)
	}
}

func (t *connectionTracker) performFlowWalk(rpt *report.Report, seenTuples *map[string]fourTuple) {
	// Consult the flowWalker for short-lived connections
	extraNodeInfo := map[string]string{
		Conntracked: "true",
	}
	t.flowWalker.walkFlows(func(f flow, alive bool) {
		tuple := fourTuple{
			f.Original.Layer3.SrcIP,
			f.Original.Layer3.DstIP,
			uint16(f.Original.Layer4.SrcPort),
			uint16(f.Original.Layer4.DstPort),
		}
		// Handle DNAT-ed short-lived connections.
		// The NAT mapper won't help since it only runs periodically,
		// missing the short-lived connections.
		if f.Original.Layer3.DstIP != f.Reply.Layer3.SrcIP {
			tuple = fourTuple{
				f.Reply.Layer3.DstIP,
				f.Reply.Layer3.SrcIP,
				uint16(f.Reply.Layer4.DstPort),
				uint16(f.Reply.Layer4.SrcPort),
			}
		}

		(*seenTuples)[tuple.key()] = tuple
		t.addConnection(rpt, tuple, "", extraNodeInfo, extraNodeInfo)
	})
}

func (t *connectionTracker) performWalkProc(rpt *report.Report, hostNodeID string, seenTuples *map[string]fourTuple) error {
	conns, err := t.conf.Scanner.Connections(t.conf.SpyProcs)
	if err != nil {
		return err
	}
	for conn := conns.Next(); conn != nil; conn = conns.Next() {
		var (
			namespaceID string
			tuple       = fourTuple{
				conn.LocalAddress.String(),
				conn.RemoteAddress.String(),
				conn.LocalPort,
				conn.RemotePort,
			}
			toNodeInfo   = map[string]string{Procspied: "true"}
			fromNodeInfo = map[string]string{Procspied: "true"}
		)
		if conn.Proc.PID > 0 {
			fromNodeInfo[process.PID] = strconv.FormatUint(uint64(conn.Proc.PID), 10)
			fromNodeInfo[report.HostNodeID] = hostNodeID
		}

		if conn.Proc.NetNamespaceID > 0 {
			namespaceID = strconv.FormatUint(conn.Proc.NetNamespaceID, 10)
		}

		// If we've already seen this connection, we should know the direction
		// (or have already figured it out), so we normalize and use the
		// canonical direction. Otherwise, we can use a port-heuristic to guess
		// the direction.
		canonical, ok := (*seenTuples)[tuple.key()]
		if (ok && canonical != tuple) || (!ok && tuple.fromPort < tuple.toPort) {
			tuple.reverse()
			toNodeInfo, fromNodeInfo = fromNodeInfo, toNodeInfo
		}
		t.addConnection(rpt, tuple, namespaceID, fromNodeInfo, toNodeInfo)
	}
	return nil
}

func (t *connectionTracker) performEbpfTrack(rpt *report.Report, hostNodeID string) error {
	t.ebpfTracker.walkConnections(func(e ebpfConnection) {
		fromNodeInfo := map[string]string{
			EBPF: "true",
		}
		toNodeInfo := map[string]string{
			EBPF: "true",
		}
		if e.pid > 0 {
			fromNodeInfo[process.PID] = strconv.Itoa(e.pid)
			fromNodeInfo[report.HostNodeID] = hostNodeID
		}

		if e.incoming {
			t.addConnection(rpt, reverse(e.tuple), e.networkNamespace, toNodeInfo, fromNodeInfo)
		} else {
			t.addConnection(rpt, e.tuple, e.networkNamespace, fromNodeInfo, toNodeInfo)
		}

	})
	return nil
}

func (t *connectionTracker) addConnection(rpt *report.Report, ft fourTuple, namespaceID string, extraFromNode, extraToNode map[string]string) {
	var (
		fromNode = t.makeEndpointNode(namespaceID, ft.fromAddr, ft.fromPort, extraFromNode)
		toNode   = t.makeEndpointNode(namespaceID, ft.toAddr, ft.toPort, extraToNode)
	)
	rpt.Endpoint = rpt.Endpoint.AddNode(fromNode.WithEdge(toNode.ID, report.EdgeMetadata{}))
	rpt.Endpoint = rpt.Endpoint.AddNode(toNode)
}

func (t *connectionTracker) makeEndpointNode(namespaceID string, addr string, port uint16, extra map[string]string) report.Node {
	portStr := strconv.Itoa(int(port))
	node := report.MakeNodeWith(
		report.MakeEndpointNodeID(t.conf.HostID, namespaceID, addr, portStr),
		map[string]string{Addr: addr, Port: portStr})
	if names := t.conf.DNSSnooper.CachedNamesForIP(addr); len(names) > 0 {
		node = node.WithSet(SnoopedDNSNames, report.MakeStringSet(names...))
	}
	if names, err := t.reverseResolver.get(addr); err == nil && len(names) > 0 {
		node = node.WithSet(ReverseDNSNames, report.MakeStringSet(names...))
	}
	if extra != nil {
		node = node.WithLatests(extra)
	}
	return node
}

func (t *connectionTracker) Stop() error {
	if t.ebpfTracker != nil {
		t.ebpfTracker.stop()
	}
	if t.flowWalker != nil {
		t.flowWalker.stop()
	}
	t.reverseResolver.stop()
	return nil
}
