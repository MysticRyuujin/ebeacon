package state

// HeadUpdate represents a new head block from any instance for a specific network.
type HeadUpdate struct {
	Network string
	Slot    uint64
	Root    string
	// Origin identifies the publishing instance so it can drop its own
	// messages, which Redis pub/sub delivers back to the publisher.
	Origin string `json:",omitempty"`
}
