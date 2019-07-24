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

// SchemaRegistryClientInterface allows applications to
// interact with Schema Registry services over HTTP.
type SchemaRegistryClientInterface interface {
	GetSchema(schemaID int) (*goavro.Codec, error)
	CreateSubject(subject string, schema string) (int, error)
	SetCredentials(username string, password string)
	EnableCaching(value bool)
}

// SchemaRegistryClient holds the information required
// to establish HTTP connections, optional authentication
// data and the caching mechanism for HTTP interactions.
type SchemaRegistryClient struct {
	schemaRegistryURL string
	credentials       *credentials
	httpClient        *http.Client
	cachingEnabled    bool
	schemaCache       map[int]*goavro.Codec
	schemaCacheLock   sync.RWMutex
	subjectCache      map[string]int
	subjectCacheLock  sync.RWMutex
}

// Error has detailed errors from Schema Registry
type Error struct {
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

type credentials struct {
	username string
	password string
}

type schemaWrapper struct {
	Schema string `json:"schema"`
}

type idWrapper struct {
	ID int `json:"id"`
}

const (
	schemaByID      = "/schemas/ids/%d"
	subjectVersions = "/subjects/%s/versions"
	contentType     = "application/vnd.schemaregistry.v1+json"
)

// CreateSchemaRegistryClient creates a client to for Schema Registry
func CreateSchemaRegistryClient(schemaRegistryURL string) *SchemaRegistryClient {
	return &SchemaRegistryClient{schemaRegistryURL: schemaRegistryURL,
		httpClient:   &http.Client{Timeout: 2 * time.Second},
		schemaCache:  make(map[int]*goavro.Codec),
		subjectCache: make(map[string]int)}
}

// GetSchema returns a goavro.Codec giving the schema id
func (client *SchemaRegistryClient) GetSchema(schemaID int) (*goavro.Codec, error) {
	if client.cachingEnabled {
		client.schemaCacheLock.RLock()
		cachedResult := client.schemaCache[schemaID]
		client.schemaCacheLock.RUnlock()
		if cachedResult != nil {
			return cachedResult, nil
		}
	}
	resp, httpErr := client.httpCall("GET", fmt.Sprintf(schemaByID, schemaID), nil)
	if httpErr != nil {
		return nil, httpErr
	}
	var schemaWrapper = new(schemaWrapper)
	parseErr := json.Unmarshal(resp, &schemaWrapper)
	if parseErr != nil {
		return nil, parseErr
	}
	codec, err := goavro.NewCodec(schemaWrapper.Schema)
	if client.cachingEnabled && err == nil {
		client.schemaCacheLock.Lock()
		client.schemaCache[schemaID] = codec
		client.schemaCacheLock.Unlock()
	}
	return codec, nil
}

// CreateSubject adds a schema to the subject if not currently cached
func (client *SchemaRegistryClient) CreateSubject(subject string, schema string) (int, error) {
	if client.cachingEnabled {
		client.subjectCacheLock.RLock()
		cachedResult := client.subjectCache[subject]
		client.subjectCacheLock.RUnlock()
		if cachedResult > 0 {
			return cachedResult, nil
		}
	}
	schemaWrapper := schemaWrapper{schema}
	schemaBytes, err := json.Marshal(schemaWrapper)
	if err != nil {
		return -1, err
	}
	payload := bytes.NewBuffer(schemaBytes)
	resp, httpErr := client.httpCall("POST", fmt.Sprintf(subjectVersions, subject), payload)
	if httpErr != nil {
		return -1, httpErr
	}
	var idWrapper = new(idWrapper)
	parseErr := json.Unmarshal(resp, &idWrapper)
	if client.cachingEnabled && parseErr == nil {
		client.subjectCacheLock.Lock()
		client.subjectCache[subject] = idWrapper.ID
		client.subjectCacheLock.Unlock()
	}
	return idWrapper.ID, parseErr
}

// SetCredentials allows users to set credentials for
// Schema Registry, in case it has been configured to
// enable HTTP authentication over secure channels.
func (client *SchemaRegistryClient) SetCredentials(username string, password string) {
	if len(username) > 0 && len(password) > 0 {
		credentials := credentials{username, password}
		client.credentials = &credentials
	}
}

// EnableCaching allows application to cache any values
// that have been returned by Schema Registry, to speed
// up performance if these values never changes.
func (client *SchemaRegistryClient) EnableCaching(value bool) {
	client.cachingEnabled = value
}

func (client *SchemaRegistryClient) httpCall(method, uri string, payload io.Reader) ([]byte, error) {
	url := fmt.Sprintf("%s%s", client.schemaRegistryURL, uri)
	req, httpErr := http.NewRequest(method, url, payload)
	if httpErr != nil {
		return nil, httpErr
	}
	if client.credentials != nil {
		req.SetBasicAuth(client.credentials.username, client.credentials.password)
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
		err := &Error{ErrorCode: resp.StatusCode, Message: resp.Status}
		return nil, err
	}
	return ioutil.ReadAll(resp.Body)
}

func (e *Error) Error() string {
	return fmt.Sprintf("%d - %s", e.ErrorCode, e.Message)
}
