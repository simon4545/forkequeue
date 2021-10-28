package levelqueue

import "time"

type Options struct {
	DataPath          string `flag:"data-path" json:"data_path"`
	MemQueueSize      int64  `flag:"mem-queue-size" json:"mem-queue-size"`
	QueueScanInterval time.Duration
	MsgTimeout        time.Duration `flag:"msg-timeout"`
}

func NewOptions() *Options {
	return &Options{
		DataPath:          `D:\goproject\forkequeue\boot\data`,
		MemQueueSize:      10000,
		QueueScanInterval: 100 * time.Millisecond,
		MsgTimeout:        60 * time.Second,
	}
}
