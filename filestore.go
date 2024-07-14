package filestore

type IFileStore interface {
	Put(data []byte, metadata map[string]interface{}) (fileId string, err error)
	Get(fileId string) (data []byte, metadata map[string]interface{}, err error)
	Delete(fileId string) error
	Close() error
}
