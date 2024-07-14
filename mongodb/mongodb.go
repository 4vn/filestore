package mongodb

import (
	"context"
	"crypto/md5"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	ChunkSize  = 8 * 1024 * 1024 // 255 * 1024 // 255 KB per chunk
	FilesColl  = "files"
	ChunksColl = "chunks"
)

type FileStore struct {
	client     *mongo.Client
	filesColl  *mongo.Collection
	chunksColl *mongo.Collection
}

func NewFileStore(uri, dbName, collPrefix string) (*FileStore, error) {
	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, err
	}

	// check connection
	err = client.Ping(context.Background(), readpref.Primary())
	if err != nil {
		return nil, err
	}

	// log.Println("Connected to MongoDB!")

	db := client.Database(dbName)
	filesCollection := db.Collection(collPrefix + FilesColl)
	chunksCollection := db.Collection(collPrefix + ChunksColl)

	// create index
	_, err = chunksCollection.Indexes().CreateOne(
		context.Background(),
		mongo.IndexModel{
			Keys: bson.D{
				{Key: "files_id", Value: 1},
				{Key: "n", Value: 1},
			},
			Options: &options.IndexOptions{Background: pBool(true), Unique: pBool(true)},
		},
		options.CreateIndexes(),
	)
	if err != nil {
		log.Fatal(err)
	}

	return &FileStore{
		client:     client,
		filesColl:  filesCollection,
		chunksColl: chunksCollection,
	}, nil
}

func (fs *FileStore) Put(buffer []byte, metadata map[string]interface{}) (fileId string, err error) {
	fileID := primitive.NewObjectID()
	uploadDate := time.Now()

	fileSize := int64(len(buffer))
	chunkNumber := 0

	md5Hash := md5.New()
	md5Hash.Write(buffer)

	for start := 0; start < len(buffer); start += ChunkSize {
		end := start + ChunkSize
		if end > len(buffer) {
			end = len(buffer)
		}

		chunkData := buffer[start:end]

		chunkDoc := bson.M{
			"files_id": fileID,
			"n":        chunkNumber,
			"data":     chunkData,
		}

		_, err := fs.chunksColl.InsertOne(context.Background(), chunkDoc)
		if err != nil {
			return "", err
		}

		chunkNumber++
	}

	fileDoc := bson.M{
		"_id":         fileID,
		"length":      fileSize,
		"chunk_size":  ChunkSize,
		"upload_date": uploadDate,
		"md5":         fmt.Sprintf("%x", md5Hash.Sum(nil)),
		// "filename":    filename,
		"metadata": metadata,
	}

	_, err = fs.filesColl.InsertOne(context.Background(), fileDoc)
	if err != nil {
		return "", err
	}

	// log.Printf("String data uploaded successfully as file %s with id %s\n", filename, fileID.Hex())
	return fileID.Hex(), nil
}

func (fs *FileStore) Get(fileId string) (data []byte, metadata map[string]interface{}, err error) {
	fileID, err := primitive.ObjectIDFromHex(fileId)
	if err != nil {
		return nil, nil, err
	}

	var fileDoc bson.M
	err = fs.filesColl.FindOne(context.Background(), bson.M{"_id": fileID}).Decode(&fileDoc)
	if err != nil {
		return nil, nil, err
	}

	//fileID := fileDoc["_id"]
	fileSize := fileDoc["length"].(int64)
	chunkSize := fileDoc["chunk_size"].(int32)
	metadata = fileDoc["metadata"].(bson.M)

	buffer := make([]byte, fileSize)
	var offset int64

	for offset < fileSize {
		var chunkDoc bson.M
		err = fs.chunksColl.FindOne(context.Background(), bson.M{"files_id": fileID, "n": offset / int64(chunkSize)}).Decode(&chunkDoc)
		if err != nil {
			return nil, nil, err
		}

		chunkData := chunkDoc["data"].(primitive.Binary).Data
		copy(buffer[offset:], chunkData)

		offset += int64(len(chunkData))
	}

	// log.Printf("File %s downloaded successfully into buffer\n", fileId)
	return buffer, metadata, nil
}

func (fs *FileStore) FastGet(fileId string) (data []byte, metadata map[string]interface{}, err error) {
	fileID, err := primitive.ObjectIDFromHex(fileId)
	if err != nil {
		return nil, nil, err
	}

	var fileDoc bson.M
	err = fs.filesColl.FindOne(context.Background(), bson.M{"_id": fileID}).Decode(&fileDoc)
	if err != nil {
		return nil, nil, err
	}

	fileSize := fileDoc["length"].(int64)
	chunkSize := fileDoc["chunk_size"].(int32)
	metadata = fileDoc["metadata"].(bson.M)

	buffer := make([]byte, fileSize)
	numChunks := (fileSize + int64(chunkSize) - 1) / int64(chunkSize)

	// Channel to collect chunk data
	type chunkResult struct {
		index int64
		data  []byte
		err   error
	}
	chunkChan := make(chan chunkResult, numChunks)

	// Fetch chunks concurrently
	for i := int64(0); i < numChunks; i++ {
		go func(chunkIndex int64) {
			var chunkDoc bson.M
			err := fs.chunksColl.FindOne(context.Background(), bson.M{"files_id": fileID, "n": chunkIndex}).Decode(&chunkDoc)
			if err != nil {
				chunkChan <- chunkResult{index: chunkIndex, data: nil, err: err}
				return
			}
			chunkData := chunkDoc["data"].(primitive.Binary).Data
			chunkChan <- chunkResult{index: chunkIndex, data: chunkData, err: nil}
		}(i)
	}

	// Collect chunk data
	for i := int64(0); i < numChunks; i++ {
		result := <-chunkChan
		if result.err != nil {
			return nil, nil, result.err
		}
		offset := result.index * int64(chunkSize)
		copy(buffer[offset:], result.data)
	}

	// log.Printf("File %s downloaded successfully into buffer\n", fileId)
	return buffer, metadata, nil
}

func (fs *FileStore) Delete(fileId string) error {
	fileID, err := primitive.ObjectIDFromHex(fileId)
	if err != nil {
		return err
	}

	_, err = fs.filesColl.DeleteOne(context.Background(), bson.M{"_id": fileID})
	if err != nil {
		return fmt.Errorf("error deleting file: %v", err)
	}

	_, err = fs.chunksColl.DeleteMany(context.Background(), bson.M{"files_id": fileID})
	if err != nil {
		return fmt.Errorf("error deleting file: %v", err)
	}

	return nil
}

func (fs *FileStore) Close() error {
	if err := fs.client.Disconnect(context.Background()); err != nil {
		return fmt.Errorf("error disconnecting from MongoDB: %v", err)
	}
	return nil
}

func pBool(v bool) *bool {
	return &v
}
