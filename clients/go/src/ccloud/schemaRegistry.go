package ccloud

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/linkedin/goavro"
)

// Error holds more detailed information about
// errors coming back from schema registry
type Error struct {
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

// SchemaRegistryClientInterface API for Schema Registry
type SchemaRegistryClientInterface interface {
	GetSchema(int) (*goavro.Codec, error)
	CreateSubject(subject string, codec *goavro.Codec) (int, error)
}

// BasicAuth holds the credentials for HTTP authentication
type basicAuth struct {
	userName string
	password string
}

// SchemaRegistryClient is a basic http client to interact with schema registry
type SchemaRegistryClient struct {
	schemaRegistryURL string
	basicAuth         *basicAuth
	httpClient        *http.Client
	schemaCache       map[int]*goavro.Codec
	schemaCacheLock   sync.RWMutex
	subjectCache      map[string]int
	subjectCacheLock  sync.RWMutex
}

type schemaResponse struct {
	Schema string `json:"schema"`
}

type idResponse struct {
	ID int `json:"id"`
}

const (
	schemaByID      = "/schemas/ids/%d"
	subjectVersions = "/subjects/%s/versions"
	contentType     = "application/vnd.schemaregistry.v1+json"
)

// CreateSchemaRegistryClient creates a client to for Schema Registry
func CreateSchemaRegistryClient(schemaRegistryURL string,
	userName string, password string) *SchemaRegistryClient {
	return &SchemaRegistryClient{schemaRegistryURL: schemaRegistryURL,
		basicAuth:    &basicAuth{userName, password},
		httpClient:   &http.Client{Timeout: 2 * time.Second},
		schemaCache:  make(map[int]*goavro.Codec),
		subjectCache: make(map[string]int)}
}

// GetSchema returns a goavro.Codec giving the schema id
func (client *SchemaRegistryClient) GetSchema(schemaID int) (*goavro.Codec, error) {
	// First check if there is an entry in the cache
	// that corresponds to the given schema id. And
	// return it as quick as possible if there is.
	client.schemaCacheLock.RLock()
	cachedResult := client.schemaCache[schemaID]
	client.schemaCacheLock.RUnlock()
	if cachedResult != nil {
		return cachedResult, nil
	}
	// If there is no entry in the cache, fetch
	// the schema using the schema id. Then, create
	// a codec from it.
	resp, httpErr := client.httpCall("GET", fmt.Sprintf(schemaByID, schemaID), nil)
	if httpErr != nil {
		return nil, httpErr
	}
	var schema = new(schemaResponse)
	parseErr := json.Unmarshal(resp, &schema)
	if parseErr != nil {
		return nil, parseErr
	}
	codec, err := goavro.NewCodec(schema.Schema)
	// Since making HTTP calls is expensive, let's
	// cache the codec (associating with the schema
	// id) so the next call executes faster. Need
	// to serialize access here since the client
	// can be invoked from multiple go routines.
	if err == nil {
		client.schemaCacheLock.Lock()
		client.schemaCache[schemaID] = codec
		client.schemaCacheLock.Unlock()
	}
	return codec, nil
}

// CreateSubject adds a schema to the subject if not currently cached
func (client *SchemaRegistryClient) CreateSubject(subject string, codec *goavro.Codec) (int, error) {
	// First check if there is a entry in the cache
	// corresponding to the subject. And return it
	// as quick as possible if there is.
	client.subjectCacheLock.RLock()
	cachedResult := client.subjectCache[subject]
	client.subjectCacheLock.RUnlock()
	if cachedResult > 0 {
		return cachedResult, nil
	}
	// If there is no entry in the cache, create
	// a text/string version of the schema so it
	// can be created in Schema Registry. This
	// will become the latest version asspcoated
	// with the subject.
	schema := schemaResponse{codec.Schema()}
	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		return -1, err
	}
	payload := bytes.NewBuffer(schemaJSON)
	resp, httpErr := client.httpCall("POST", fmt.Sprintf(subjectVersions, subject), payload)
	if httpErr != nil {
		return -1, httpErr
	}
	var id = new(idResponse)
	parseErr := json.Unmarshal(resp, &id)
	// Since making HTTP calls is expensive, let's
	// cache the schema id (associating with the
	// subject) so the next call executes faster.
	// Need to serialize access here since the client
	// can be invoked from multiple go routines.
	if parseErr == nil {
		client.subjectCacheLock.Lock()
		client.subjectCache[subject] = id.ID
		client.subjectCacheLock.Unlock()
	}
	return id.ID, parseErr
}

func (client *SchemaRegistryClient) httpCall(method, uri string, payload io.Reader) ([]byte, error) {
	url := fmt.Sprintf("%s%s", client.schemaRegistryURL, uri)
	req, httpErr := http.NewRequest(method, url, payload)
	if httpErr != nil {
		return nil, httpErr
	}
	if len(client.basicAuth.userName) > 0 && len(client.basicAuth.password) > 0 {
		req.SetBasicAuth(client.basicAuth.userName, client.basicAuth.password)
	}
	req.Header.Set("Content-Type", contentType)
	resp, execErr := client.httpClient.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if execErr != nil {
		return nil, execErr
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		err := &Error{}
		parseErr := json.NewDecoder(resp.Body).Decode(&err)
		if parseErr != nil {
			return nil, &Error{resp.StatusCode,
				"Unrecognized error found"}
		}
		return nil, err
	}
	return ioutil.ReadAll(resp.Body)
}

func (e *Error) Error() string {
	return fmt.Sprintf("%d - %s", e.ErrorCode, e.Message)
}
