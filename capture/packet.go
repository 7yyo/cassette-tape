package capture

type packet struct {
	seq     uint32
	from    string
	payload []byte
}
