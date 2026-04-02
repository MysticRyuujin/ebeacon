package state

import "sync/atomic"

// LocalState is an in-process implementation of SharedState (single-instance mode).
type LocalState struct {
	finalizedEpoch atomic.Uint64
	headCh         chan HeadUpdate
}

// NewLocalState creates a LocalState.
func NewLocalState() *LocalState {
	return &LocalState{
		headCh: make(chan HeadUpdate, 16),
	}
}

func (l *LocalState) PublishHead(network string, slot uint64, root string) {
	select {
	case l.headCh <- HeadUpdate{Network: network, Slot: slot, Root: root}:
	default:
	}
}

func (l *LocalState) SubscribeHead() <-chan HeadUpdate {
	return l.headCh
}

func (l *LocalState) PublishFinalized(epoch uint64) {
	for {
		old := l.finalizedEpoch.Load()
		if epoch <= old {
			return
		}
		if l.finalizedEpoch.CompareAndSwap(old, epoch) {
			return
		}
	}
}

func (l *LocalState) GetFinalized() uint64 {
	return l.finalizedEpoch.Load()
}

func (l *LocalState) Close() error {
	close(l.headCh)
	return nil
}
