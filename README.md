# Filestore [![GoDoc](https://godoc.org/github.com/4vn/filestore?status.svg)](https://godoc.org/github.com/4vn/filestore)

### Import

``` go
import "github.com/4vn/filestore/mongodb"
```

### Usage

``` go
fs, _ := mongodb.NewFileStore("mongodb://localhost:27017", "testdb", "myfs.")
defer fs.Close()

data := []byte(`hello world!`)

fileId, err := fs.Put(data, bson.M{"filename": "hello.txt"})
if err != nil {
	log.Fatalf("Error uploading file: %v", err)
}
log.Printf("File uploaded with ID: %s\n", fileId)

downloadedData, downloadedMetadata, err := fs.Get(fileId)
if err != nil {
	log.Fatalf("Error downloading file: %v", err)
}

log.Printf("Downloaded data: %s\n", downloadedData)
log.Printf("Downloaded metadata: %v\n", downloadedMetadata)

if err := fs.Delete(fileId); err != nil {
	panic(err)
}
```

### License

MIT
