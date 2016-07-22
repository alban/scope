package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/containernetworking/cni/pkg/ns"
	docker "github.com/fsouza/go-dockerclient"
)

// add an event listener for getting new containers

// from the list of containers, generate the container topology with
// all the running containers having additional controls

// remember the state of tcd for each container, there will be four
// states I think - uninstalled, slow, medium, fast

// will have to vendor the pkg from cni for entering network namespace

type state int

const (
	created state = iota
	running
	stopped
	destroyed
)

type container struct {
	state state
	pid   int
}

type plugin struct {
	client *docker.Client

	containerLock sync.RWMutex
	containers    map[string]container
}

const (
	socket = "/var/run/scope/plugins/tcd.sock"
)

func main() {
	os.Remove(socket)
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	go func() {
		<-interrupt
		os.Remove(socket)
		os.Exit(0)
	}()

	listener, err := net.Listen("unix", socket)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	log.Printf("Listening on: unix://%s", socket)

	dc, err := docker.NewClient("unix:///var/run/docker.sock")
	if err != nil {
		log.Fatal(err)
	}
	plugin := newPlugin(dc)
	http.HandleFunc("/report", plugin.Report)
	http.HandleFunc("/control", plugin.Control)
	if err := http.Serve(listener, nil); err != nil {
		log.Printf("error: %v", err)
	}
}

func newPlugin(dc *docker.Client) *plugin {
	p := &plugin{
		client:     dc,
		containers: map[string]container{},
	}
	go func() {
		for {
			p.loopIteration()
			time.Sleep(time.Second)
		}
	}()
	return p
}

func (p *plugin) loopIteration() {
	events := make(chan *docker.APIEvents)
	if err := p.client.AddEventListener(events); err != nil {
		log.Error(err)
		return
	}
	defer func() {
		if err := p.client.RemoveEventListener(events); err != nil {
			log.Error(err)
		}
	}()
	if err := p.getContainers(); err != nil {
		log.Error(err)
		return
	}
	for {
		event, ok := <-events
		if !ok {
			log.Error("event listener unexpectedly disconnected")
			return
		}
		p.handleEvent(event)
	}
}

func (p *plugin) getContainers() error {
	apiContainers, err := p.client.ListContainers(docker.ListContainersOptions{All: true})
	if err != nil {
		return err
	}

	for _, apiContainer := range apiContainers {
		containerState, err := p.getContainerState(apiContainer.ID)
		if err != nil {
			log.Error(err)
			continue
		}
		state := destroyed
		switch {
		case containerState.Dead || containerState.Paused || containerState.Restarting || containerState.OOMKilled:
			state = stopped
		case containerState.Running:
			state = running
		}
		p.updateContainer(apiContainer.ID, state, containerState.Pid)
	}

	return nil
}

func (p *plugin) handleEvent(event *docker.APIEvents) {
	var (
		state state
		pid   int
	)
	switch event.Status {
	case "create":
		state = created
		tmpPid, err := p.getContainerPID(event.ID)
		if err != nil {
			log.Error(err)
			return
		}
		pid = tmpPid
	case "destroy":
		state = destroyed
	case "start", "unpause":
		state = running
	case "die", "pause":
		state = stopped
	default:
		return
	}
	p.updateContainer(event.ID, state, pid)
}

func (p *plugin) getContainerPID(containerID string) (int, error) {
	dockerContainer, err := p.getContainer(containerID)
	if dockerContainer == nil {
		return 0, err
	}
	return dockerContainer.State.Pid, nil
}

func (p *plugin) getContainerState(containerID string) (*docker.State, error) {
	dockerContainer, err := p.getContainer(containerID)
	if dockerContainer == nil {
		return nil, err
	}
	return &dockerContainer.State, nil
}

func (p *plugin) getContainer(containerID string) (*docker.Container, error) {
	dockerContainer, err := p.client.InspectContainer(containerID)
	if err != nil {
		if _, ok := err.(*docker.NoSuchContainer); !ok {
			return nil, err
		}
		return nil, nil
	}
	return dockerContainer, nil
}

func (p *plugin) updateContainer(containerID string, state state, pid int) {
	p.containerLock.Lock()
	defer p.containerLock.Unlock()
	if state == destroyed {
		delete(p.containers, containerID)
		return
	}
	cont := container{}
	if c, found := p.containers[containerID]; found {
		cont = c
		cont.state = state
	} else {
		cont = container{
			state: state,
			pid:   pid,
		}
	}
	p.containers[containerID] = cont
}

type report struct {
	Container topology
	Plugins   []pluginSpec
}

type topology struct {
	Nodes    map[string]node    `json:"nodes"`
	Controls map[string]control `json:"controls"`
}

type node struct {
	LatestControls map[string]controlEntry `json:"latestControls,omitempty"`
}

type controlEntry struct {
	Timestamp time.Time   `json:"timestamp"`
	Value     controlData `json:"value"`
}

type controlData struct {
	Dead bool `json:"dead"`
}

type control struct {
	ID    string `json:"id"`
	Human string `json:"human"`
	Icon  string `json:"icon"`
	Rank  int    `json:"rank"`
}

type pluginSpec struct {
	ID          string   `json:"id"`
	Label       string   `json:"label"`
	Description string   `json:"description,omitempty"`
	Interfaces  []string `json:"interfaces"`
	APIVersion  string   `json:"api_version,omitempty"`
}

func (p *plugin) Report(w http.ResponseWriter, r *http.Request) {
	rpt := &report{
		Container: topology{
			Nodes:    p.getContainerNodes(),
			Controls: getTcdControls(),
		},
		Plugins: []pluginSpec{
			{
				ID:          "tcd",
				Label:       "Traffic control",
				Description: "Adds traffic control to the running Docker containers",
				Interfaces:  []string{"reporter", "controller"},
				APIVersion:  "1",
			},
		},
	}
	raw, err := json.Marshal(rpt)
	if err != nil {
		log.Printf("error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(raw)
}

// states:
// created, destroyed - don't create any node
// running, not running - create node with controls

func (p *plugin) getContainerNodes() map[string]node {
	p.containerLock.RLock()
	defer p.containerLock.RUnlock()
	nodes := map[string]node{}
	timestamp := time.Now()
	for ID, c := range p.containers {
		dead := false
		switch c.state {
		case created, destroyed:
			// do nothing, to prevent adding a stale node
			// to a report
		case stopped:
			dead = true
			fallthrough
		case running:
			nodeID := fmt.Sprintf("%s;<container>", ID)
			nodes[nodeID] = node{
				LatestControls: getTcdNodeControls(timestamp, dead),
			}
		}
	}
	return nodes
}

func getTcdNodeControls(timestamp time.Time, dead bool) map[string]controlEntry {
	controls := map[string]controlEntry{}
	entry := controlEntry{
		Timestamp: timestamp,
		Value: controlData{
			Dead: dead,
		},
	}
	for _, c := range getControls() {
		controls[c.control.ID] = entry
	}
	return controls
}

func getTcdControls() map[string]control {
	controls := map[string]control{}
	for _, c := range getControls() {
		controls[c.control.ID] = c.control
	}
	return controls
}

type extControl struct {
	control control
	handler func(pid int) error
}

func getControls() []extControl {
	return []extControl{
		{
			control: control{
				ID:    "slow",
				Human: "Traffic speed: slow",
				Icon:  "fa-hourglass-1",
				Rank:  20,
			},
			handler: func(pid int) error {
				return doTCD(pid, 2000, 0, 800000)
			},
		},
		{
			control: control{
				ID:    "medium",
				Human: "Traffic speed: medium",
				Icon:  "fa-hourglass-2",
				Rank:  21,
			},
			handler: func(pid int) error {
				return doTCD(pid, 300, 0, 800000)
			},
		},
		{
			control: control{
				ID:    "fast",
				Human: "Traffic speed: fast",
				Icon:  "fa-hourglass-3",
				Rank:  22,
			},
			handler: func(pid int) error {
				return doTCD(pid, 1, 0, 800000)
			},
		},
	}
}

func doTCD(pid int, delay, loss, rate uint32) error {
	cmds := [][]string{
		split("tc qdisc replace dev eth0 root handle 1: netem"),
		split("ip link add ifb0 type ifb"),
		split("ip link set ifb0 up"),
		split("tc qdisc add dev eth0 handle ffff: ingress"),
		split("tc filter add dev eth0 parent ffff: protocol ip u32 match u32 0 0 action mirred egress redirect dev ifb0"),
		split("tc qdisc replace dev ifb0 handle 1:0 root netem"),

		split(fmt.Sprintf("tc qdisc change dev eth0 root handle 1: netem delay %dms loss %d%% rate %dkbit", delay, loss, rate)),
	}
	netNS := fmt.Sprintf("/proc/%d/ns/net", pid)
	err := ns.WithNetNSPath(netNS, func(hostNS ns.NetNS) error {
		for _, cmd := range cmds {
			if output, err := exec.Command(cmd[0], cmd[1:]...).CombinedOutput(); err != nil {
				log.Error(string(output))
				//return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func split(cmd string) []string {
	return strings.Split(cmd, " ")
}

type request struct {
	NodeID  string
	Control string
}

type response struct {
	Error string `json:"error,omitempty"`
}

func (p *plugin) Control(w http.ResponseWriter, r *http.Request) {
	p.containerLock.RLock()
	defer p.containerLock.RUnlock()
	xreq := request{}
	err := json.NewDecoder(r.Body).Decode(&xreq)
	if err != nil {
		log.Printf("Bad request: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	containerID, err := getContainerIDFromNodeID(xreq.NodeID)
	if err != nil {
		log.Printf("Bad request: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	container, found := p.containers[containerID]
	if !found {
		msg := fmt.Sprintf("Container %s not found", containerID)
		log.Print(msg)
		http.Error(w, msg, http.StatusNotFound)
		return
	}
	var (
		handler func(pid int) error
		res     response
	)
	for _, c := range getControls() {
		if c.control.ID == xreq.Control {
			handler = c.handler
			break
		}
	}
	if handler != nil {
		if err := handler(container.pid); err != nil {
			res.Error = err.Error()
		}
	} else {
		res.Error = fmt.Sprintf("unknown control %s", xreq.Control)
	}
	raw, err := json.Marshal(res)
	if err != nil {
		log.Printf("Internal server error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(raw)
}

func getContainerIDFromNodeID(nodeID string) (string, error) {
	if !strings.HasSuffix(nodeID, ";<container>") {
		return "", fmt.Errorf("%s is not a container node", nodeID)
	}
	return strings.TrimSuffix(nodeID, ";<container>"), nil
}
