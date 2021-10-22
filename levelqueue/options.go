package levelqueue

type Options struct {
	DataPath string `flag:"data-path" json:"data_path"`
}

func NewOptions() *Options {
	return &Options{
		DataPath: `D:\goproject\forkequeue\boot\data`,
	}
}
