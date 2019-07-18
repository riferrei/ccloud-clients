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
	timeout         = 2 * time.Second
)

// CreateSchemaRegistryClient creates a client to for Schema Registry
func CreateSchemaRegistryClient(schemaRegistryURL string,
	userName string, password string) *SchemaRegistryClient {
	return &SchemaRegistryClient{schemaRegistryURL: schemaRegistryURL,
		basicAuth:    &basicAuth{userName, password},
		httpClient:   &http.Client{Timeout: timeout},
		schemaCache:  make(map[int]*goavro.Codec),
		subjectCache: make(map[string]int)}
}

// GetSchema returns a goavro.Codec giving the schema id
func (client *SchemaRegistryClient) GetSchema(schemaID int) (*goavro.Codec, error) {
	client.schemaCacheLock.RLock()
	cachedResult := client.schemaCache[schemaID]
	client.schemaCacheLock.RUnlock()
	if cachedResult != nil {
		return cachedResult, nil
	}
	resp, err := client.httpCall("GET", fmt.Sprintf(schemaByID, schemaID), nil)
	if err != nil {
		return nil, err
	}
	schema, err := parseSchema(resp)
	if err != nil {
		return nil, err
	}
	codec, err := goavro.NewCodec(schema.Schema)
	client.schemaCacheLock.Lock()
	client.schemaCache[schemaID] = codec
	client.schemaCacheLock.Unlock()
	return codec, nil
}

// CreateSubject adds a schema to the subject if not currently cached
func (client *SchemaRegistryClient) CreateSubject(subject string, codec *goavro.Codec) (int, error) {
	client.subjectCacheLock.RLock()
	cachedResult := client.subjectCache[subject]
	client.subjectCacheLock.RUnlock()
	if cachedResult > 0 {
		return cachedResult, nil
	}
	schema := schemaResponse{codec.Schema()}
	json, err := json.Marshal(schema)
	if err != nil {
		return 0, err
	}
	payload := bytes.NewBuffer(json)
	resp, err := client.httpCall("POST", fmt.Sprintf(subjectVersions, subject), payload)
	if err != nil {
		return 0, err
	}
	return parseID(resp)
}

func (client *SchemaRegistryClient) httpCall(method, uri string, payload io.Reader) ([]byte, error) {
	url := fmt.Sprintf("%s%s", client.schemaRegistryURL, uri)
	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		return nil, err
	}
	if client.basicAuth != nil {
		req.SetBasicAuth(client.basicAuth.userName, client.basicAuth.password)
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := client.httpClient.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}
	if !okStatus(resp) {
		return nil, newError(resp)
	}
	return ioutil.ReadAll(resp.Body)
}

func parseID(str []byte) (int, error) {
	var id = new(idResponse)
	err := json.Unmarshal(str, &id)
	return id.ID, err
}

func parseSchema(str []byte) (*schemaResponse, error) {
	var schema = new(schemaResponse)
	err := json.Unmarshal(str, &schema)
	return schema, err
}

func okStatus(resp *http.Response) bool {
	return resp.StatusCode >= 200 && resp.StatusCode < 400
}

func (e *Error) Error() string {
	return fmt.Sprintf("%d - %s", e.ErrorCode, e.Message)
}

func newError(resp *http.Response) *Error {
	err := &Error{}
	parsingErr := json.NewDecoder(resp.Body).Decode(&err)
	if parsingErr != nil {
		return &Error{resp.StatusCode, "Unrecognized error found"}
	}
	return err
}
