package state

import "sync"

// LocalState is an in-process implementation of SharedState (single-instance mode).
type LocalState struct {
	mu        sync.Mutex
	finalized map[string]uint64
	closed    bool
	headCh    chan HeadUpdate
}

// NewLocalState creates a LocalState.
func NewLocalState() *LocalState {
	return &LocalState{
		finalized: make(map[string]uint64),
		headCh:    make(chan HeadUpdate, 16),
	}
}

func (l *LocalState) PublishHead(network string, slot uint64, root string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	select {
	case l.headCh <- HeadUpdate{Network: network, Slot: slot, Root: root}:
	default:
	}
}

func (l *LocalState) SubscribeHead() <-chan HeadUpdate {
	return l.headCh
}

func (l *LocalState) PublishFinalized(network string, epoch uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if epoch > l.finalized[network] {
		l.finalized[network] = epoch
	}
}

func (l *LocalState) GetFinalized(network string) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.finalized[network]
}

func (l *LocalState) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.closed {
		l.closed = true
		close(l.headCh)
	}
	return nil
}
