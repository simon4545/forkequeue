package levelqueue

import "time"

type Options struct {
	DataPath          string `flag:"data-path" json:"data_path"`
	MemQueueSize      int64  `flag:"mem-queue-size" json:"mem-queue-size"`
	QueueScanInterval time.Duration
	MsgTimeout        time.Duration `flag:"msg-timeout"`

	HTTPAddress string `flag:"http-address"`
}

func NewOptions() *Options {
	return &Options{
		DataPath:          `D:\goproject\forkequeue\boot\data`,
		MemQueueSize:      10000,
		QueueScanInterval: 100 * time.Millisecond,
		MsgTimeout:        60 * time.Second,
		HTTPAddress:       "0.0.0.0:8989",
	}
}
