package mgstore

import (
	"context"
	"errors"

	apperror "github.com/F1zm0n/pipefile-storage/storage/error"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var (
	ErrMongoConnection    = errors.New("error connecting to mongo")
	ErrMongoIndexCreation = errors.New("error creating mongo index on field")
)

type MongoStorageConfig struct {
	uri        string
	collection string
	database   string
	creds      MongoStorageCredentials
}

type MongoStorageCredentials struct {
	Username string
	Password string
}

type MongoOpt func(config *MongoStorageConfig)

func WithUri(uri string) MongoOpt {
	return func(config *MongoStorageConfig) {
		config.uri = uri
	}
}

func WithCollection(col string) MongoOpt {
	return func(config *MongoStorageConfig) {
		config.collection = col
	}
}

func WithDatabase(db string) MongoOpt {
	return func(config *MongoStorageConfig) {
		config.database = db
	}
}

func WithAuthentication(creds MongoStorageCredentials) MongoOpt {
	return func(config *MongoStorageConfig) {
		config.creds = creds
	}
}

type MongoStorage struct {
	col *mongo.Collection
}

// NewMongoStorageConfig has default values
// uri:        "mongodb://localhost:27017",
// collection: "pipefile",
// database:   "pipefile",
func NewMongoStorageConfig(opts ...MongoOpt) MongoStorageConfig {
	cfg := &MongoStorageConfig{
		uri:        "mongodb://localhost:27017",
		collection: "pipefile",
		database:   "pipefile",
		creds: MongoStorageCredentials{
			Password: "admin",
			Username: "admin",
		},
	}
	for _, o := range opts {
		o(cfg)
	}

	return *cfg
}

func NewMongoStorage(ctx context.Context, cfg MongoStorageConfig) (MongoStorage, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(cfg.uri).SetAuth(options.Credential{
		AuthMechanism: "SCRAM-SHA-256",
		Username:      cfg.creds.Username,
		Password:      cfg.creds.Password,
	}))

	if err != nil {
		return MongoStorage{}, errors.Join(ErrMongoConnection, err)
	}

	collection := client.Database(cfg.database).Collection(cfg.collection)

	storage := MongoStorage{
		col: collection,
	}

	if err = storage.createIndex(ctx, "key"); err != nil {
		return MongoStorage{}, err
	}

	return storage, nil
}

func (m MongoStorage) createIndex(ctx context.Context, fieldName string) error {
	idxModel := mongo.IndexModel{
		Keys: bson.D{{fieldName, 1}},
	}
	_, err := m.col.Indexes().CreateOne(ctx, idxModel)

	return errors.Join(ErrMongoIndexCreation, err)
}

type PipefileModel struct {
	Key      string `bson:"key"`
	FileData []byte `bson:"file_data"`
}

func (m MongoStorage) Get(ctx context.Context, key string) ([]byte, error) {

	var mod PipefileModel

	err := m.col.FindOne(ctx, bson.D{{"key", key}}).Decode(&mod)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, apperror.ErrEntryNotFound
		}
		return nil, errors.Join(apperror.ErrUnknownStorageError, err)
	}

	return mod.FileData, nil
}

func (m MongoStorage) Put(ctx context.Context, key string, data []byte) error {
	mod := PipefileModel{
		Key:      key,
		FileData: data,
	}
	_, err := m.col.InsertOne(ctx, mod)
	if err != nil {
		return errors.Join(apperror.ErrUnknownStorageError, err)
	}

	return nil
}

func (m MongoStorage) Close(ctx context.Context) error {
	return m.col.Database().Client().Disconnect(ctx)
}
