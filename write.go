package statistica

// WriteRepository common write interface.
type WriteRepository interface {
	AddRows(...*ItemRow) error
}

func (r *SQLRepository) AddRows(...*ItemRow) error {
	// TODO collect by key and store
	return nil
}
