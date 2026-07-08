package state

// HeadUpdate represents a new head block from any instance for a specific network.
type HeadUpdate struct {
	Network string
	Slot    uint64
	Root    string
}

// SharedState coordinates state across multiple eBeacon instances.
type SharedState interface {
	PublishHead(network string, slot uint64, root string)
	SubscribeHead() <-chan HeadUpdate
	PublishFinalized(network string, epoch uint64)
	GetFinalized(network string) uint64
	Close() error
}
