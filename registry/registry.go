package registry

import (
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// GeeRegistry is a simple register center, provide following functions.
// add a server and receive heartbeat to keep it alive.
// returns all alive servers and delete dead servers sync simultaneously.
type GeeRegistry struct {
	timeout time.Duration // 0 means no limit
	mutex   sync.Mutex    // protect following
	servers map[string]*ServerItem
}

type ServerItem struct {
	Addr  string
	start time.Time
}

const (
	DefaultPath    = "/_geerpc_/registry"
	defaultTimeout = 5 * time.Minute
)

func New(timeout time.Duration) *GeeRegistry {
	return &GeeRegistry{
		timeout: timeout,
		servers: make(map[string]*ServerItem),
	}
}

var DefaultRegistry = New(defaultTimeout)

func (r *GeeRegistry) addServer(addr string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if s, ok := r.servers[addr]; ok {
		s.start = time.Now() // if exists, update start time to keep alive
		return
	}
	r.servers[addr] = &ServerItem{Addr: addr, start: time.Now()}
}

func (r *GeeRegistry) getAliveServers() []string {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	var servers []string
	for addr, s := range r.servers {
		if r.timeout == 0 || s.start.Add(r.timeout).After(time.Now()) {
			servers = append(servers, addr)
		} else {
			delete(r.servers, addr)
		}
	}
	sort.Strings(servers)
	return servers
}

// Runs at /_geerpc_/registry.
// Get: Returns a list of all available services, carried by the custom field X-Geerpc-Servers.
// Post: Adds a service instance or sends a heartbeat, which is carried by the custom field X-Geerpc-Server.
func (r *GeeRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		// to keep it simple, servers message is set in req.Header
		w.Header().Set("X-Geerpc-Servers", strings.Join(r.getAliveServers(), ","))
	case "POST":
		// to keep it simple, server message is set in req.Header
		addr := req.Header.Get("X-Geerpc-Server")
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		r.addServer(addr)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// HandleHTTP registers an HTTP handler for GeeRegistry messages on registryPath
func (r *GeeRegistry) HandleHTTP(registryPath string) {
	http.Handle(registryPath, r)
	log.Println("rpc registry path:", registryPath)
}

func HandleHTTP() {
	DefaultRegistry.HandleHTTP(DefaultPath)
}

// Heartbeat send a heartbeat message every once in a while
// it's a helper function for a server to register or send heartbeat
func Heartbeat(registryAddr, addr string, duration time.Duration) {
	if duration == 0 {
		// make sure there is enough time to send heart beat
		// before it's removed from registry
		duration = defaultTimeout - time.Duration(1)*time.Minute
	}
	registry := registryAddr + DefaultPath
	err := sendHeartbeat(registry, addr)
	go func() {
		t := time.NewTicker(duration)
		for err == nil {
			<-t.C
			err = sendHeartbeat(registry, addr)
		}
	}()
}

func sendHeartbeat(registry, addr string) error {
	log.Println(addr, "send heart beat to registry", registry)
	req, _ := http.NewRequest("POST", registry, nil)
	req.Header.Set("X-Geerpc-Server", addr)
	httpClient := http.Client{}
	if _, err := httpClient.Do(req); err != nil {
		log.Println("rpc server: send heart beat err:", err)
		return err
	}
	return nil
}
