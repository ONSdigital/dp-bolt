package mock

type RowValues struct {
	Data []interface{}
	Meta map[string]interface{}
	Err  error
}

type RowsStub struct {
	Rows  []RowValues
	index int
}

func (s *RowsStub) Next() ([]interface{}, map[string]interface{}, error) {
	data := s.Rows[s.index].Data
	meta := s.Rows[s.index].Meta
	err := s.Rows[s.index].Err
	s.index++
	return data, meta, err
}