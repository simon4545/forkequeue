package levelqueue

type Options struct {
	DataPath     string `flag:"data-path" json:"data_path"`
	MemQueueSize int64  `flag:"mem-queue-size" json:"mem-queue-size"`
}

func NewOptions() *Options {
	return &Options{
		DataPath:     `D:\goproject\forkequeue\boot\data`,
		MemQueueSize: 10000,
	}
}
