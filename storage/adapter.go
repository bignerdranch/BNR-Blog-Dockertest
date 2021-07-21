package storage

// Adapter describes the storage interface for phone number management
type Adapter interface {
	CreatePhoneNumber(number string) (int, error)
	GetPhoneNumbers() ([]PhoneNumber, error)
	RemovePhoneNumber(id int) error
	UpdatePhoneNumber(number PhoneNumber) error
}
