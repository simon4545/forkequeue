package levelqueue

import "time"

type Options struct {
	DataPath     string `flag:"data-path" json:"data_path"`
	MemQueueSize int64  `flag:"mem-queue-size" json:"mem-queue-size"`

	PendingScanInterval        time.Duration
	PendingScanRefreshInterval time.Duration
	PendingScanSelectionCount  int `flag:"pending-scan-selection-count"`
	PendingScanWorkerPoolMax   int `flag:"pending-scan-worker-pool-max"`
	PendingScanDirtyPercent    float64

	MsgTimeout time.Duration `flag:"msg-timeout"`

	HTTPAddress string `flag:"http-address"`
}

func NewOptions() *Options {
	return &Options{
		DataPath: "./data",

		PendingScanInterval:        100 * time.Millisecond,
		PendingScanRefreshInterval: 5 * time.Second,
		PendingScanSelectionCount:  20,
		PendingScanWorkerPoolMax:   4,
		PendingScanDirtyPercent:    0.25,

		MsgTimeout:  60 * time.Second,
		HTTPAddress: "0.0.0.0:8989",
	}
}
