package serf

import (
	"fmt"
	"path"
	"strings"
	"sync"

	"github.com/epsniff/expodb/pkg/config"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

// const (
// 	serverSerfCheckInterval = 10 * time.Second
// 	serverSerfCheckTimeout  = 3 * time.Second
// )

// EventHandler is a handler that does things when events happen.
type EventHandler interface {
	HandleEvent(serf.Event)
}

// Agent starts and manages a Serf instance, adding some niceties
// on top of Serf such as storing logs that you can later retrieve,
// and invoking EventHandlers when events occur.
type Agent struct {
	// Stores the serf configuration
	conf *serf.Config

	// eventCh is used for Serf to deliver events on
	eventCh chan serf.Event

	// eventHandlers is the registered handlers for events
	eventHandlers     map[EventHandler]struct{}
	eventHandlerList  []EventHandler
	eventHandlersLock sync.Mutex

	logger *zap.Logger

	// This is the underlying Serf we are wrapping
	serf *serf.Serf

	// shutdownCh is used for shutdowns
	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

// Start creates a new agent, potentially returning an error
func New(config *config.Config, logger *zap.Logger) (*Agent, error) {
	snapshotPath := path.Join(config.SerfDataDir, "serf_snapshot.serf")

	// Create a channel to listen for events from Serf
	eventCh := make(chan serf.Event, 64)
	conf, err := createSerfConfig(config, logger, eventCh, snapshotPath)
	if err != nil {
		return nil, fmt.Errorf("creating serf config for agent failed: err;%v", err)
		return nil, err
	}

	// Setup the agent
	agent := &Agent{
		conf:          conf,
		eventCh:       eventCh,
		eventHandlers: make(map[EventHandler]struct{}),
		logger:        logger,
		shutdownCh:    make(chan struct{}),
	}

	return agent, nil
}

// Start is used to initiate the event listeners. It is separate from
// create so that there isn't a race condition between creating the
// agent and registering handlers
func (a *Agent) Start() error {

	a.logger.Info("Serf agent starting")

	// Create serf first
	serf, err := serf.Create(a.conf)
	if err != nil {
		return fmt.Errorf("Serf Agent: Error creating Serf: %s", err)
	}
	a.serf = serf

	// Start event loop
	go a.eventLoop()
	return nil
}

// Leave prepares for a graceful shutdown of the agent and its processes
func (a *Agent) Leave() error {
	if a.serf == nil {
		return nil
	}

	a.logger.Info("requesting graceful leave from Serf")
	return a.serf.Leave()
}

// Shutdown closes this agent and all of its processes. Should be preceded
// by a Leave for a graceful shutdown.
func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.shutdown {
		return nil
	}

	if a.serf == nil {
		goto EXIT
	}

	a.logger.Info("requesting serf shutdown")
	if err := a.serf.Shutdown(); err != nil {
		return err
	}

EXIT:
	a.logger.Info("shutdown complete")
	a.shutdown = true
	close(a.shutdownCh)
	return nil
}

// ShutdownCh returns a channel that can be selected to wait
// for the agent to perform a shutdown.
func (a *Agent) ShutdownCh() <-chan struct{} {
	return a.shutdownCh
}

// Returns the Serf agent of the running Agent.
func (a *Agent) Serf() *serf.Serf {
	return a.serf
}

// Returns the Serf config of the running Agent.
func (a *Agent) SerfConfig() *serf.Config {
	return a.conf
}

// Join asks the Serf instance to join. See the Serf.Join function.
func (a *Agent) Join(addrs []string, replay bool) (n int, err error) {
	ignoreOld := !replay
	n, err = a.serf.Join(addrs, ignoreOld)
	if n > 0 {
		a.logger.Info("joining cluster", zap.Int("node-count", n), zap.Strings("joining", addrs), zap.Bool("replay", replay))
	}
	if err != nil {
		a.logger.Error("error joining", zap.Error(err))
	}
	return
}

// ForceLeave is used to eject a failed node from the cluster
func (a *Agent) ForceLeave(node string) error {
	a.logger.Info("Force leaving (aka ejecting) node", zap.String("node", node))
	err := a.serf.RemoveFailedNode(node)
	if err != nil {
		a.logger.Warn("failed to remove/eject node", zap.Error(err))
	}
	return err
}

// UserEvent sends a UserEvent on Serf, see Serf.UserEvent.
func (a *Agent) UserEvent(name string, payload []byte, coalesce bool) error {
	a.logger.Info("Requesting user event send",
		zap.String("name", name),
		zap.Bool("coalesced", coalesce), zap.ByteString("payload", payload))
	err := a.serf.UserEvent(name, payload, coalesce)
	if err != nil {
		a.logger.Warn("failed to send user event", zap.Error(err))
	}
	return err
}

// Query sends a Query on Serf, see Serf.Query.
func (a *Agent) Query(name string, payload []byte, params *serf.QueryParam) (*serf.QueryResponse, error) {
	// Prevent the use of the internal prefix
	if strings.HasPrefix(name, serf.InternalQueryPrefix) {
		// Allow the special "ping" query
		if name != serf.InternalQueryPrefix+"ping" || payload != nil {
			return nil, fmt.Errorf("Queries cannot contain the '%s' prefix", serf.InternalQueryPrefix)
		}
	}
	a.logger.Debug("Requesting query send", zap.String("name", name), zap.ByteString("payload", payload))
	resp, err := a.serf.Query(name, payload, params)
	if err != nil {
		a.logger.Warn("Failed to start user query", zap.Error(err))
	}
	return resp, err
}

// RegisterEventHandler adds an event handler to receive event notifications
func (a *Agent) RegisterEventHandler(eh EventHandler) {
	a.eventHandlersLock.Lock()
	defer a.eventHandlersLock.Unlock()

	a.eventHandlers[eh] = struct{}{}
	a.eventHandlerList = nil
	for eh := range a.eventHandlers {
		a.eventHandlerList = append(a.eventHandlerList, eh)
	}
}

// DeregisterEventHandler removes an EventHandler and prevents more invocations
func (a *Agent) DeregisterEventHandler(eh EventHandler) {
	a.eventHandlersLock.Lock()
	defer a.eventHandlersLock.Unlock()

	delete(a.eventHandlers, eh)
	a.eventHandlerList = nil
	for eh := range a.eventHandlers {
		a.eventHandlerList = append(a.eventHandlerList, eh)
	}
}

// eventLoop listens to events from Serf and fans out to event handlers
func (a *Agent) eventLoop() {
	serfShutdownCh := a.serf.ShutdownCh()

	for {
		select {
		case e := <-a.eventCh:
			a.logger.Info("Received event", zap.String("event", e.String()))
			a.eventHandlersLock.Lock()
			handlers := a.eventHandlerList
			a.eventHandlersLock.Unlock()
			for _, eh := range handlers {
				eh.HandleEvent(e)
			}

		case <-serfShutdownCh:
			a.logger.Warn("Serf shutdown detected, quitting")
			a.Shutdown()
			return

		case <-a.shutdownCh:
			return
		}
	}
}

// Stats is used to get various runtime information and stats
func (a *Agent) Stats() map[string]map[string]string {
	local := a.serf.LocalMember()
	event_handlers := make(map[string]string)

	output := map[string]map[string]string{
		"agent": map[string]string{
			"name": local.Name,
		},
		"serf":           a.serf.Stats(),
		"tags":           local.Tags,
		"event_handlers": event_handlers,
	}
	return output
}
