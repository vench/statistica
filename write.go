package statistica

// WriteRepository common write interface.
type WriteRepository interface {
	// AddRows add rows.
	AddRows(rows ...*ItemRow) error
}

func (r *SQLRepository) AddRows(_ ...*ItemRow) error {
	// @TODO: collect by key and store
	return nil
}
