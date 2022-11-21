package xclient

import (
	reg "github.com/heejinzzz/geerpc/registry"
	"log"
	"net/http"
	"strings"
	"time"
)

// GeeRegistryDiscovery is a discovery for multi servers with a registry center
type GeeRegistryDiscovery struct {
	*MultiServersDiscovery
	registry   string
	timeout    time.Duration
	lastUpdate time.Time
}

const defaultUpdateTimeout = 10 * time.Second

func NewGeeRegistryDiscovery(registry string, timeout time.Duration) *GeeRegistryDiscovery {
	if timeout == 0 {
		timeout = defaultUpdateTimeout
	}
	return &GeeRegistryDiscovery{
		MultiServersDiscovery: NewMultiServersDiscovery([]string{}),
		registry:              registry + reg.DefaultPath,
		timeout:               timeout,
	}
}

func (d *GeeRegistryDiscovery) Update(servers []string) error {
	d.rwMutex.Lock()
	defer d.rwMutex.Unlock()
	d.servers = servers
	d.lastUpdate = time.Now()
	return nil
}

func (d *GeeRegistryDiscovery) Refresh() error {
	d.rwMutex.Lock()
	defer d.rwMutex.Unlock()
	if d.lastUpdate.Add(d.timeout).After(time.Now()) {
		return nil
	}
	log.Println("rpc registry: refresh servers from registry", d.registry)
	res, err := http.Get(d.registry)
	if err != nil {
		log.Println("rpc registry refresh err:", err)
		return err
	}
	servers := strings.Split(res.Header.Get("X-Geerpc-Servers"), ",")
	d.servers = make([]string, 0, len(servers))
	for _, server := range servers {
		if strings.TrimSpace(server) != "" {
			d.servers = append(d.servers, server)
		}
	}
	d.lastUpdate = time.Now()
	return nil
}

func (d *GeeRegistryDiscovery) Get(mode SelectMode) (string, error) {
	if err := d.Refresh(); err != nil {
		return "", err
	}
	return d.MultiServersDiscovery.Get(mode)
}

func (d *GeeRegistryDiscovery) GetAll() ([]string, error) {
	if err := d.Refresh(); err != nil {
		return nil, err
	}
	return d.MultiServersDiscovery.GetAll()
}
