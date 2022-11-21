package xclient

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

type SelectMode int

const (
	RandomSelect     SelectMode = iota // select randomly
	RoundRobinSelect                   // select using Robbin algorithm
)

type Discovery interface {
	Refresh() error // refresh from remote registry center
	Update(servers []string) error
	Get(mode SelectMode) (string, error)
	GetAll() ([]string, error)
}

// MultiServersDiscovery is a discovery for multi servers without a registry center,
// user provides the server addresses explicitly instead
type MultiServersDiscovery struct {
	rand    *rand.Rand   // generate random number
	rwMutex sync.RWMutex // protect following
	servers []string
	index   int // record the selected position for robin algorithm
}

// NewMultiServersDiscovery creates a MultiServersDiscovery instance
func NewMultiServersDiscovery(servers []string) *MultiServersDiscovery {
	d := &MultiServersDiscovery{
		rand:    rand.New(rand.NewSource(time.Now().UnixNano())),
		servers: servers,
	}
	d.index = d.rand.Intn(math.MaxInt32 - 1)
	return d
}

// Refresh doesn't make sense for MultiServersDiscovery, so ignore it
func (d *MultiServersDiscovery) Refresh() error {
	return nil
}

// Update the servers of discovery dynamically if needed
func (d *MultiServersDiscovery) Update(servers []string) error {
	d.rwMutex.Lock()
	defer d.rwMutex.Unlock()
	d.servers = servers
	return nil
}

// Get a server according to mode
func (d *MultiServersDiscovery) Get(mode SelectMode) (string, error) {
	d.rwMutex.Lock()
	defer d.rwMutex.Unlock()
	n := len(d.servers)
	if n == 0 {
		return "", errors.New("rpc discovery: no available servers")
	}
	switch mode {
	case RandomSelect:
		return d.servers[d.rand.Intn(n)], nil
	case RoundRobinSelect:
		res := d.servers[d.index%n] // servers could be updated, so mode n to ensure safety
		d.index = (d.index + 1) % n
		return res, nil
	default:
		return "", fmt.Errorf("rpc discovery: unsupported select mode: %d", mode)
	}
}

func (d *MultiServersDiscovery) GetAll() ([]string, error) {
	d.rwMutex.RLock()
	defer d.rwMutex.RUnlock()
	// return a copy of d.servers
	servers := make([]string, len(d.servers))
	copy(servers, d.servers)
	return servers, nil
}
