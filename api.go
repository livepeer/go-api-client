package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/eventials/go-tus"
	"github.com/golang/glog"
	"github.com/livepeer/go-api-client/logs"
	"github.com/livepeer/go-api-client/metrics"
	"github.com/tomnomnom/linkheader"
)

const (
	SHORT    = 4
	DEBUG    = 5
	VERBOSE  = 6
	VVERBOSE = 7
	INSANE   = 12
	INSANE2  = 14
)

var (
	// ErrNotExists returned if receives a 404 error from the API
	ErrNotExists = errors.New("not found")
	// ErrRateLimited returned if receives a 429 error from the API
	ErrRateLimited = errors.New("rate limited")
)

var defaultHTTPClient = &http.Client{
	Timeout: 4 * time.Second,
}

var longTimeoutHTTPClient = &http.Client{
	Timeout: 2 * time.Minute,
}

var hostName, _ = os.Hostname()

const (
	livepeerAPIGeolocateURL = "http://livepeer.com/api/geolocate"
	ProdServer              = "livepeer.com"

	RecordingStatusWaiting = "waiting"
	RecordingStatusReady   = "ready"
)

type NFTMetadataTemplate string

const (
	NFTMetadataTemplatePlayer NFTMetadataTemplate = "player" // default
	NFTMetadataTemplateFile   NFTMetadataTemplate = "file"
)

type TaskPhase string

const (
	TaskPhasePending   TaskPhase = "pending"
	TaskPhaseWaiting   TaskPhase = "waiting"
	TaskPhaseRunning   TaskPhase = "running"
	TaskPhaseFailed    TaskPhase = "failed"
	TaskPhaseCompleted TaskPhase = "completed"
)

type (
	// Object with all options given to Livepeer API
	ClientOptions struct {
		Server      string
		AccessToken string
		UserAgent   string
		Timeout     time.Duration
		Presets     []string
		Metrics     metrics.APIRecorder
	}

	// API object incapsulating Livepeer's hosted API
	Client struct {
		accessToken string
		userAgent   string
		presets     []string
		metrics     metrics.APIRecorder

		chosenServer string
		httpClient   *http.Client
		broadcasters []string
	}

	geoResp struct {
		ChosenServer string `json:"chosenServer,omitempty"`
		Servers      []struct {
			Server   string `json:"server,omitempty"`
			Duration int    `json:"duration,omitempty"`
		} `json:"servers,omitempty"`
	}

	CreateStreamReq struct {
		Name     string   `json:"name,omitempty"`
		ParentID string   `json:"parentId,omitempty"`
		Presets  []string `json:"presets,omitempty"`
		// one of
		// - P720p60fps16x9
		// - P720p30fps16x9
		// - P720p30fps4x3
		// - P576p30fps16x9
		// - P360p30fps16x9
		// - P360p30fps4x3
		// - P240p30fps16x9
		// - P240p30fps4x3
		// - P144p30fps16x9
		Profiles            []Profile `json:"profiles,omitempty"`
		Record              bool      `json:"record,omitempty"`
		RecordObjectStoreId string    `json:"recordObjectStoreId,omitempty"`
	}

	// Profile transcoding profile
	Profile struct {
		Name    string `json:"name,omitempty"`
		Width   int    `json:"width,omitempty"`
		Height  int    `json:"height,omitempty"`
		Bitrate int    `json:"bitrate,omitempty"`
		Fps     int    `json:"fps"`
		FpsDen  int    `json:"fpsDen,omitempty"`
		Gop     string `json:"gop,omitempty"`
		Profile string `json:"profile,omitempty"` // enum: - H264Baseline - H264Main - H264High - H264ConstrainedHigh
		Encoder string `json:"encoder,omitempty"` // enum: - h264, h265, vp8, vp9
	}

	MultistreamTargetRef struct {
		Profile   string `json:"profile,omitempty"`
		VideoOnly bool   `json:"videoOnly,omitempty"`
		ID        string `json:"id,omitempty"`
	}

	// Stream object as returned by the API
	Stream struct {
		ID                         string    `json:"id,omitempty"`
		Name                       string    `json:"name,omitempty"`
		Presets                    []string  `json:"presets,omitempty"`
		Kind                       string    `json:"kind,omitempty"`
		UserID                     string    `json:"userId,omitempty"`
		StreamKey                  string    `json:"streamKey,omitempty"`
		PlaybackID                 string    `json:"playbackId,omitempty"`
		ParentID                   string    `json:"parentId,omitempty"`
		CreatedAt                  int64     `json:"createdAt,omitempty"`
		LastSeen                   int64     `json:"lastSeen,omitempty"`
		SourceSegments             int64     `json:"sourceSegments,omitempty"`
		TranscodedSegments         int64     `json:"transcodedSegments,omitempty"`
		SourceSegmentsDuration     float64   `json:"sourceSegmentsDuration,omitempty"`
		TranscodedSegmentsDuration float64   `json:"transcodedSegmentsDuration,omitempty"`
		Deleted                    bool      `json:"deleted,omitempty"`
		Record                     bool      `json:"record"`
		Profiles                   []Profile `json:"profiles,omitempty"`
		Multistream                struct {
			Targets []MultistreamTargetRef `json:"targets,omitempty"`
		} `json:"multistream"`

		// These can be present on parent stream objects if they are used to stream
		// directly to broadcasters (i.e. not using the streamKey through RTMP)
		RecordingStatus string `json:"recordingStatus,omitempty"` // ready, waiting
		RecordingURL    string `json:"recordingUrl,omitempty"`
		Mp4Url          string `json:"mp4Url,omitempty"`
	}

	// UserSession user's sessions
	UserSession struct {
		Stream
	}

	MultistreamTarget struct {
		ID        string `json:"id,omitempty"`
		URL       string `json:"url,omitempty"`
		Name      string `json:"name,omitempty"`
		UserID    string `json:"userId,omitempty"`
		Disabled  bool   `json:"disabled,omitempty"`
		CreatedAt int64  `json:"createdAt,omitempty"`
	}

	Task struct {
		ID            string `json:"id"`
		UserID        string `json:"userId"`
		CreatedAt     int64  `json:"createdAt"`
		InputAssetID  string `json:"inputAssetId,omitempty"`
		OutputAssetID string `json:"outputAssetId,omitempty"`
		Type          string `json:"type"`
		Params        struct {
			Upload        *UploadTaskParams        `json:"upload"`
			Import        *UploadTaskParams        `json:"import"`
			Export        *ExportTaskParams        `json:"export"`
			Transcode     *TranscodeTaskParams     `json:"transcode"`
			TranscodeFile *TranscodeFileTaskParams `json:"transcode-file"`
		} `json:"params"`
		Output *struct {
			Export *struct {
				IPFS *struct {
					VideoFileCid          string `json:"videoFileCid"`
					NftMetadataCid        string `json:"nftMetadataCid"`
					VideoFileUrl          string `json:"videoFileUrl"`
					VideoFileGatewayUrl   string `json:"videoFileGatewayUrl"`
					NftMetadataUrl        string `json:"nftMetadataUrl"`
					NftMetadataGatewayUrl string `json:"nftMetadataGatewayUrl"`
				} `json:"ipfs"`
			} `json:"export"`
		} `json:"output"`
		Status TaskStatus `json:"status"`
	}

	TaskOnlyId struct {
		ID string `json:"id"`
	}

	TaskStatus struct {
		Phase        TaskPhase `json:"phase"`
		Progress     float64   `json:"progress"`
		Retries      int       `json:"retries,omitempty"`
		UpdatedAt    int64     `json:"updatedAt,omitempty"`
		ErrorMessage string    `json:"errorMessage,omitempty"`
	}

	UploadTaskParams struct {
		URL                      string `json:"url,omitempty"`
		RecordedSessionID        string `json:"recordedSessionId,omitempty"`
		UploadedObjectKey        string `json:"uploadedObjectKey,omitempty"`
		CatalystPipelineStrategy string `json:"catalystPipelineStrategy,omitempty"`
	}

	ExportTaskParams struct {
		Custom *struct {
			URL     string            `json:"url"`
			Method  string            `json:"method,omitempty"`
			Headers map[string]string `json:"headers,omitempty"`
		} `json:"custom,omitempty"`
		IPFS *struct {
			Pinata *struct {
				JWT       string `json:"jwt,omitempty"`
				APIKey    string `json:"apiKey,omitempty"`
				APISecret string `json:"apiSecret,omitempty"`
			} `json:"pinata,omitempty"`
			NFTMetadataTemplate `json:"nftMetadataTemplate,omitempty"`
			NFTMetadata         map[string]interface{} `json:"nftMetadata,omitempty"`
		} `json:"ipfs,omitempty"`
	}

	TranscodeTaskParams struct {
		Profile Profile `json:"profile,omitempty"`
	}

	TranscodeFileTaskParams struct {
		Input struct {
			URL string `json:"url"`
		} `json:"input"`
		Storage struct {
			URL string `json:"url"`
		} `json:"storage"`
		Outputs struct {
			HLS struct {
				Path string `json:"path"`
			} `json:"hls"`
		} `json:"outputs"`
		Profiles                 []Profile `json:"profiles,omitempty"`
		CatalystPipelineStrategy string    `json:"catalystPipelineStrategy,omitempty"`
	}

	updateTaskProgressRequest struct {
		Status TaskStatus `json:"status"`
	}

	uploadViaURLRequest struct {
		Name                     string `json:"name,omitempty"`
		URL                      string `json:"url"`
		CatalystPipelineStrategy string `json:"catalystPipelineStrategy,omitempty"`
	}

	requestUploadRequest struct {
		Name                     string `json:"name,omitempty"`
		CatalystPipelineStrategy string `json:"catalystPipelineStrategy,omitempty"`
	}

	transcodeAssetRequest struct {
		Name    string  `json:"name,omitempty"`
		Profile Profile `json:"profile,omitempty"`
	}

	TaskAndAsset struct {
		Asset Asset `json:"asset"`
		Task  Task  `json:"task"`
	}

	UploadUrls struct {
		Url         string     `json:"url"`
		TusEndpoint string     `json:"tusEndpoint"`
		Asset       Asset      `json:"asset"`
		Task        TaskOnlyId `json:"task"`
	}

	Pinata struct {
		JWT       string `json:"jwt,omitempty"`
		APIKey    string `json:"apiKey,omitempty"`
		APISecret string `json:"apiSecret,omitempty"`
	}

	IPFS struct {
		Pinata      *Pinata     `json:"pinata,omitempty"`
		NFTMetadata interface{} `json:"nftMetadata,omitempty"`
	}

	exportAssetRequest struct {
		IPFS *IPFS `json:"ipfs,omitempty"`
	}

	ExportAssetResp struct {
		Task Task `json:"task"`
	}

	Asset struct {
		ID            string      `json:"id"`
		Deleted       bool        `json:"deleted,omitempty"`
		PlaybackID    string      `json:"playbackId"`
		UserID        string      `json:"userId"`
		CreatedAt     int64       `json:"createdAt"`
		SourceAssetID string      `json:"sourceAssetId,omitempty"`
		ObjectStoreID string      `json:"objectStoreId"`
		DownloadURL   string      `json:"downloadUrl"`
		PlaybackURL   string      `json:"playbackUrl"`
		Status        AssetStatus `json:"status"`
		AssetSpec
	}

	AssetStatus struct {
		Phase        string `json:"phase"`
		UpdatedAt    int64  `json:"updatedAt,omitempty"`
		ErrorMessage string `json:"errorMessage,omitempty"`
	}

	AssetSpec struct {
		Name                string          `json:"name,omitempty"`
		Type                string          `json:"type"`
		Size                uint64          `json:"size"`
		Hash                []AssetHash     `json:"hash"`
		Files               []AssetFile     `json:"files"`
		VideoSpec           *AssetVideoSpec `json:"videoSpec,omitempty"`
		Storage             AssetStorage    `json:"storage"`
		PlaybackRecordingID string          `json:"playbackRecordingId,omitempty"`
	}

	AssetFile struct {
		Type string `json:"type"`
		Path string `json:"path"`
	}

	AssetHash struct {
		Hash      string `json:"hash"`
		Algorithm string `json:"algorithm"`
	}

	AssetVideoSpec struct {
		Format      string        `json:"format"`
		DurationSec float64       `json:"duration"`
		Bitrate     float64       `json:"bitrate,omitempty"`
		Tracks      []*AssetTrack `json:"tracks"`
	}

	AssetTrack struct {
		Type        string  `json:"type"`
		Codec       string  `json:"codec"`
		StartTime   float64 `json:"startTime,omitempty"`
		DurationSec float64 `json:"duration,omitempty"`
		Bitrate     float64 `json:"bitrate,omitempty"`
		// video track fields
		Width       int     `json:"width,omitempty"`
		Height      int     `json:"height,omitempty"`
		PixelFormat string  `json:"pixelFormat,omitempty"`
		FPS         float64 `json:"fps,omitempty"`
		// auido track fields
		Channels   int `json:"channels,omitempty"`
		SampleRate int `json:"sampleRate,omitempty"`
		BitDepth   int `json:"bitDepth,omitempty"`
	}

	AssetStorage struct {
		IPFS   *AssetIPFS `json:"ipfs,omitempty"`
		Status struct {
			Phase        string            `json:"phase"`
			ErrorMessage string            `json:"errorMessage,omitempty"`
			Tasks        map[string]string `json:"tasks"`
		} `json:"status,omitempty"`
	}

	AssetIPFS struct {
		IPFSFileInfo
		NFTMetadata *IPFSFileInfo `json:"nftMetadata,omitempty"`
		Spec        struct {
			NFTMetadataTemplate `json:"nftMetadataTemplate,omitempty"`
			NFTMetadata         map[string]interface{} `json:"nftMetadata,omitempty"`
		} `json:"spec"`
	}

	IPFSFileInfo struct {
		CID        string `json:"cid"`
		GatewayUrl string `json:"gatewayUrl,omitempty"`
		Url        string `json:"url,omitempty"`
	}

	ObjectStore struct {
		ID        string `json:"id"`
		UserId    string `json:"userId"`
		CreatedAt int64  `json:"createdAt"`
		URL       string `json:"url"`
		Name      string `json:"name,omitempty"`
		PublicURL string `json:"publicUrl,omitempty"`
		Disabled  bool   `json:"disabled"`
	}

	// // Profile ...
	// Profile struct {
	// 	Fps     int    `json:"fps"`
	// 	Name    string `json:"name,omitempty"`
	// 	Width   int    `json:"width,omitempty"`
	// 	Height  int    `json:"height,omitempty"`
	// 	Bitrate int    `json:"bitrate,omitempty"`
	// }

	addressResp struct {
		Address string `json:"address"`
	}

	setActiveReq struct {
		Active    bool   `json:"active"`
		HostName  string `json:"hostName"`
		StartedAt int64  `json:"startedAt"`
	}

	deactivateManyReq struct {
		IDS []string `json:"ids"`
	}

	deactivateManyResp struct {
		RowCount int `json:"rowCount"`
	}

	ListOptions struct {
		Limit                    int
		Cursor                   string
		AllUsers, IncludeDeleted bool
		Filters                  map[string]interface{}
		Order                    map[string]bool
	}

	TranscodeFileReq struct {
		Input   TranscodeFileReqInput   `json:"input,omitempty"`
		Storage TranscodeFileReqStorage `json:"storage,omitempty"`
		Outputs TranscodeFileReqOutputs `json:"outputs,omitempty"`
	}

	TranscodeFileReqInput struct {
		Url         string                       `json:"url,omitempty"`
		Type        string                       `json:"type,omitempty"`
		Endpoint    string                       `json:"endpoint,omitempty"`
		Credentials *TranscodeFileReqCredentials `json:"credentials,omitempty"`
		Bucket      string                       `json:"bucket,omitempty"`
		Path        string                       `json:"path,omitempty"`
	}

	TranscodeFileReqStorage struct {
		Type        string                       `json:"type,omitempty"`
		Endpoint    string                       `json:"endpoint,omitempty"`
		Credentials *TranscodeFileReqCredentials `json:"credentials,omitempty"`
		Bucket      string                       `json:"bucket,omitempty"`
	}

	TranscodeFileReqOutputs struct {
		Hls TranscodeFileReqOutputsHls `json:"hls,omitempty"`
	}

	TranscodeFileReqOutputsHls struct {
		Path string `json:"path,omitempty"`
	}

	TranscodeFileReqCredentials struct {
		AccessKeyId     string `json:"accessKeyId,omitempty"`
		SecretAccessKey string `json:"secretAccessKey,omitempty"`
	}
)

// NewAPIClientGeolocated creates a new Livepeer API object calling the
// geolocation endpoint if no server is provided (by default, server is
// production instead)
func NewAPIClientGeolocated(opts ClientOptions) (*Client, string) {
	if opts.Server == "" {
		opts.Server = MustGeolocateAPIServer()
	}
	return NewAPIClient(opts), opts.Server
}

// NewAPIClient creates new Livepeer API object with a full configuration.
func NewAPIClient(opts ClientOptions) *Client {
	if opts.Server == "" {
		opts.Server = ProdServer
	}
	httpClient := defaultHTTPClient
	if opts.Timeout != 0 {
		httpClient = &http.Client{
			Timeout: opts.Timeout,
		}
	}
	if opts.Metrics == nil {
		opts.Metrics = metrics.APIRequestRecorderFunc(func(name string, duration time.Duration, err error) {})
	}
	return &Client{
		chosenServer: addScheme(opts.Server),
		accessToken:  opts.AccessToken,
		userAgent:    opts.UserAgent,
		presets:      opts.Presets,
		metrics:      opts.Metrics,
		httpClient:   httpClient,
	}
}

func addScheme(uri string) string {
	if uri == "" {
		return uri
	}
	luri := strings.ToLower(uri)
	if strings.HasPrefix(luri, "http://") || strings.HasPrefix(luri, "https://") {
		return uri
	}
	if strings.Contains(luri, ".local") || strings.HasPrefix(luri, "localhost") {
		return "http://" + uri
	}
	return "https://" + uri
}

// GeolocateAPIServer calls geolocation API endpoint to find the closest server.
func GeolocateAPIServer() (string, error) {
	resp, err := http.Get(livepeerAPIGeolocateURL)
	if err != nil {
		return "", fmt.Errorf("error geolocating Livepeer API server (%s): %w", livepeerAPIGeolocateURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("status error contacting Livepeer API server (%s) status %d body: %s", livepeerAPIGeolocateURL, resp.StatusCode, string(b))
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("error geolocating Livepeer API server (%s): %w", livepeerAPIGeolocateURL, err)
	}
	glog.Info(string(b))
	geo := &geoResp{}
	err = json.Unmarshal(b, geo)
	if err != nil {
		return "", err
	}
	glog.Infof("chosen server: %s, servers num: %d", geo.ChosenServer, len(geo.Servers))
	return addScheme(geo.ChosenServer), nil
}

func MustGeolocateAPIServer() string {
	server, err := GeolocateAPIServer()
	if err != nil {
		glog.Fatalf("Failed to geolocate API server: %v", err)
	}
	return server
}

// GetServer returns chosen server
func (lapi *Client) GetServer() string {
	return lapi.chosenServer
}

// Broadcasters returns list of hostnames of broadcasters to use
func (lapi *Client) Broadcasters() ([]string, error) {
	u := fmt.Sprintf("%s/api/broadcaster", lapi.chosenServer)
	var broadcasters []addressResp
	err := lapi.getJSON(u, "broadcaster", "", &broadcasters)
	if err != nil {
		glog.Errorf("Error getting broadcasters from Livepeer API server (%s) error: %v", u, err)
		return nil, err
	}
	addresses := make([]string, 0, len(broadcasters))
	for _, a := range broadcasters {
		addresses = append(addresses, a.Address)
	}
	return addresses, nil
}

// Ingest object
type Ingest struct {
	Base     string `json:"base,omitempty"`
	Playback string `json:"playback,omitempty"`
	Ingest   string `json:"ingest,omitempty"`
}

// Ingest returns ingest object
func (lapi *Client) Ingest(all bool) ([]Ingest, error) {
	u := fmt.Sprintf("%s/api/ingest", lapi.chosenServer)
	if all {
		u += "?first=false"
	}
	var ingests []Ingest
	err := lapi.getJSON(u, "ingest", "", &ingests)
	if err != nil {
		glog.Errorf("Error getting ingests from Livepeer API server (%s) error: %v", u, err)
		return nil, err
	}
	return ingests, nil
}

var StandardProfiles = []Profile{
	{
		Name:    "240p0",
		Fps:     0,
		Bitrate: 250000,
		Width:   426,
		Height:  240,
		Gop:     "2.0",
	},
	{
		Name:    "360p0",
		Fps:     0,
		Bitrate: 800000,
		Width:   640,
		Height:  360,
		Gop:     "2.0",
	},
	{
		Name:    "480p0",
		Fps:     0,
		Bitrate: 1600000,
		Width:   854,
		Height:  480,
		Gop:     "2.0",
	},
	{
		Name:    "720p0",
		Fps:     0,
		Bitrate: 3000000,
		Width:   1280,
		Height:  720,
		Gop:     "2.0",
	},
}

// DeleteStream deletes stream
func (lapi *Client) DeleteStream(id string) error {
	glog.V(logs.DEBUG).Infof("Deleting Livepeer stream '%s' ", id)
	u := fmt.Sprintf("%s/api/stream/%s", lapi.chosenServer, id)
	err := lapi.doRequest("DELETE", u, "stream", "", nil, nil)
	return err
}

// CreateStreamEx creates stream with specified name and profiles
func (lapi *Client) CreateStreamEx(name string, record bool, presets []string, profiles ...Profile) (*Stream, error) {
	return lapi.CreateStream(CreateStreamReq{Name: name, Record: record, Presets: presets, Profiles: profiles})
}

// CreateStream creates stream with specified name and profiles
func (lapi *Client) CreateStream(csr CreateStreamReq) (*Stream, error) {
	if csr.Name == "" {
		return nil, errors.New("stream must have a name")
	}
	if len(csr.Presets) == 0 && len(csr.Profiles) == 0 {
		csr.Profiles = StandardProfiles
	}
	glog.V(logs.DEBUG).Infof(`Creating Livepeer stream name=%q presets="%v" profiles="%+v"`, csr.Name, csr.Presets, csr.Profiles)
	u := fmt.Sprintf("%s/api/stream", lapi.chosenServer)
	if csr.ParentID != "" {
		u = fmt.Sprintf("%s/api/stream/%s/stream", lapi.chosenServer, csr.ParentID)
	}
	var stream *Stream
	err := lapi.doRequest("POST", u, "stream", "", csr, &stream)
	if err != nil {
		return nil, err
	}
	glog.Infof("Created stream name=%q id=%s", csr.Name, stream.ID)
	return stream, nil
}

// DefaultPresets returns default presets
func (lapi *Client) DefaultPresets() []string {
	return lapi.presets
}

// GetStreamByKey gets stream by streamKey
func (lapi *Client) GetStreamByKey(key string) (*Stream, error) {
	if key == "" {
		return nil, errors.New("empty key")
	}
	u := fmt.Sprintf("%s/api/stream/key/%s?main=true", lapi.chosenServer, key)
	return lapi.getStream(u, "get_by_key")
}

// GetStreamByPlaybackID gets stream by playbackID
func (lapi *Client) GetStreamByPlaybackID(playbackID string) (*Stream, error) {
	if playbackID == "" {
		return nil, errors.New("empty playbackID")
	}
	u := fmt.Sprintf("%s/api/stream/playback/%s", lapi.chosenServer, playbackID)
	return lapi.getStream(u, "get_by_playbackid")
}

// GetStream gets stream by id
func (lapi *Client) GetStream(id string, forceRecordingUrl bool) (*Stream, error) {
	if id == "" {
		return nil, errors.New("empty id")
	}
	u := fmt.Sprintf("%s/api/stream/%s", lapi.chosenServer, id)
	if forceRecordingUrl {
		u += "?forceUrl=1"
	}
	return lapi.getStream(u, "get_by_id")
}

func (lapi *Client) GetSessionsNew(id string, forceUrl bool) ([]UserSession, error) {
	if id == "" {
		return nil, errors.New("empty id")
	}
	u := fmt.Sprintf("%s/api/session?parentId=%s", lapi.chosenServer, id)
	if forceUrl {
		u += "&forceUrl=1"
	}
	var sessions []UserSession
	err := lapi.getJSON(u, "session", "list_sessions", &sessions)
	if err != nil {
		glog.Errorf("Error getting sessions for stream by id from Livepeer API server (%s) error: %v", u, err)
		return nil, err
	}
	return sessions, nil
}

// GetSessions gets user's sessions for the stream by id
func (lapi *Client) GetSessions(id string, forceUrl bool) ([]UserSession, error) {
	if id == "" {
		return nil, errors.New("empty id")
	}
	u := fmt.Sprintf("%s/api/stream/%s/sessions", lapi.chosenServer, id)
	if forceUrl {
		u += "?forceUrl=1"
	}

	var sessions []UserSession
	err := lapi.getJSON(u, "stream", "list_child_streams", &sessions)
	if err != nil {
		glog.Errorf("Error getting sessions for stream by id from Livepeer API server (%s) error: %v", u, err)
		return nil, err
	}
	return sessions, nil
}

// SetActive set isActive
func (lapi *Client) SetActive(id string, active bool, startedAt time.Time) (bool, error) {
	if id == "" {
		return false, errors.New("empty id")
	}
	u := fmt.Sprintf("%s/api/stream/%s/setactive", lapi.chosenServer, id)
	req := setActiveReq{
		Active:   active,
		HostName: hostName,
	}
	if !startedAt.IsZero() {
		req.StartedAt = startedAt.UnixNano() / int64(time.Millisecond)
	}
	var res json.RawMessage
	err := lapi.doRequest("PUT", u, "stream", "set_active", req, &res)
	glog.Infof("Ran setactive request id=%s request=%+v response=%q error=%q", id, req, res, err)
	if err != nil {
		lapi.metrics.APIRequest("set_active", 0, err)
		return false, err
	}
	return true, nil
}

// DeactivateMany sets many streams isActive field to false
func (lapi *Client) DeactivateMany(ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, errors.New("empty ids")
	}
	u := fmt.Sprintf("%s/api/stream/deactivate-many", lapi.chosenServer)
	req := deactivateManyReq{
		IDS: ids,
	}
	var res deactivateManyResp
	err := lapi.doRequest("PATCH", u, "stream", "deactivate_many", req, &res)
	if err != nil {
		return 0, err
	}
	return res.RowCount, nil
}

func (lapi *Client) getStream(url, metricName string) (*Stream, error) {
	var stream *Stream
	if err := lapi.getJSON(url, "stream", metricName, &stream); err != nil {
		return nil, err
	} else if stream == nil {
		// API return null if stream does not exists
		return nil, ErrNotExists
	}
	return stream, nil
}

func (lapi *Client) GetTask(id string, strongConsistency bool) (*Task, error) {
	var task Task
	url := fmt.Sprintf("%s/api/task/%s", lapi.chosenServer, id)
	if strongConsistency {
		url += "?strongConsistency=1"
	}
	if err := lapi.getJSON(url, "task", "", &task); err != nil {
		return nil, err
	}
	return &task, nil
}

func (lapi *Client) UpdateTaskStatus(id string, phase TaskPhase, progress float64) error {
	var (
		url    = fmt.Sprintf("%s/api/task/%s/status", lapi.chosenServer, id)
		input  = &updateTaskProgressRequest{Status: TaskStatus{phase, progress, 0, 0, ""}}
		output json.RawMessage
	)
	err := lapi.doRequest("POST", url, "task", "update_task_progress", input, &output)
	if err != nil {
		return err
	}
	glog.V(logs.DEBUG).Infof("Updated task progress id=%s phase=%s progress=%v output=%q", id, phase, progress, string(output))
	return nil
}

func (lapi *Client) UploadViaURL(url, name, catalystStrategy string) (*Asset, *Task, error) {
	var (
		requestUrl = fmt.Sprintf("%s/api/asset/import", lapi.chosenServer)
		input      = &uploadViaURLRequest{
			URL:                      url,
			Name:                     name,
			CatalystPipelineStrategy: catalystStrategy,
		}
		output TaskAndAsset
	)
	err := lapi.doRequest("POST", requestUrl, "import_asset", "", input, &output)
	if err != nil {
		return nil, nil, err
	}
	glog.V(logs.DEBUG).Infof("Created import task id=%s assetId=%s status=%s type=%s", output.Task.ID, output.Asset.ID, output.Task.Status.Phase, output.Task.Type)
	return &output.Asset, &output.Task, nil
}

func (lapi *Client) RequestUpload(name, catalystStrategy string) (*UploadUrls, error) {
	var (
		requestUrl = fmt.Sprintf("%s/api/asset/request-upload", lapi.chosenServer)
		input      = &requestUploadRequest{
			Name:                     name,
			CatalystPipelineStrategy: catalystStrategy,
		}
		output UploadUrls
	)
	err := lapi.doRequest("POST", requestUrl, "request_upload", "", input, &output)
	if err != nil {
		return nil, err
	}
	glog.V(logs.DEBUG).Infof("Created request upload for task id=%s assetId=%s", output.Task.ID, output.Asset.ID)
	return &output, nil
}

func (lapi *Client) UploadAsset(ctx context.Context, url string, file io.ReadSeeker) error {
	return doWithRetries("upload_asset", 2, isRetriable, func() error {
		_, err := file.Seek(0, io.SeekStart)
		if err != nil {
			return fmt.Errorf("error seeking to start of file: %w", err)
		}
		req, err := http.NewRequestWithContext(ctx, "PUT", url, io.NopCloser(file))
		if err != nil {
			return err
		}

		startTime := time.Now()
		resp, err := longTimeoutHTTPClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if err := checkResponseError(resp); err != nil {
			return err
		}
		glog.V(logs.DEBUG).Infof("Uploaded asset to url=%s dur=%v", url, time.Since(startTime))
		return nil
	})
}

// Temporary function while waiting for go-api-client to get fixed
func (lapi *Client) ResumableUpload(url string, file *os.File) error {
	config := tus.DefaultConfig()
	config.ChunkSize = 8 * 1024 * 1024 // 8MB

	client, err := tus.NewClient(url, config)
	if err != nil {
		return err
	}
	upload, err := tus.NewUploadFromFile(file)
	if err != nil {
		return err
	}
	uploader, err := client.CreateUpload(upload)
	if err != nil {
		return err
	}

	err = uploader.Upload()

	return err
}

func (lapi *Client) TranscodeAsset(assetId string, name string, profile Profile) (*Asset, *Task, error) {
	var (
		url    = fmt.Sprintf("%s/api/asset/%s/transcode", lapi.chosenServer, assetId)
		input  = &transcodeAssetRequest{Name: name, Profile: profile}
		output TaskAndAsset
	)
	err := lapi.doRequest("POST", url, "transcode_asset", "", input, &output)
	if err != nil {
		return nil, nil, err
	}
	glog.V(logs.DEBUG).Infof("Created transcode task id=%s assetId=%s status=%s type=%s", output.Task.ID, output.Asset.ID, output.Task.Status.Phase, output.Task.Type)
	return &output.Asset, &output.Task, nil
}

func (lapi *Client) ExportAsset(assetId string) (*Task, error) {
	var (
		url    = fmt.Sprintf("%s/api/asset/%s/export", lapi.chosenServer, assetId)
		input  = &exportAssetRequest{IPFS: &IPFS{}}
		output ExportAssetResp
	)
	err := lapi.doRequest("POST", url, "export_asset", "", input, &output)
	if err != nil {
		return nil, err
	}
	glog.V(logs.DEBUG).Infof("Created export task id=%s status=%s type=%s", output.Task.ID, output.Task.Status.Phase, output.Task.Type)
	return &output.Task, nil
}

func (lapi *Client) GetMultistreamTarget(id string) (*MultistreamTarget, error) {
	var target MultistreamTarget
	url := fmt.Sprintf("%s/api/multistream/target/%s", lapi.chosenServer, id)
	if err := lapi.getJSON(url, "multistream_target", "", &target); err != nil {
		return nil, err
	}
	return &target, nil
}

func (lapi *Client) GetAsset(id string, strongConsistency bool) (*Asset, error) {
	var asset Asset
	url := fmt.Sprintf("%s/api/asset/%s", lapi.chosenServer, id)
	if strongConsistency {
		url += "?strongConsistency=1"
	}
	if err := lapi.getJSON(url, "asset", "", &asset); err != nil {
		return nil, err
	}
	return &asset, nil
}

func (lapi *Client) ListAssets(opts ListOptions) ([]*Asset, string, error) {
	if opts.Limit <= 0 {
		opts.Limit = 10
	}
	url := fmt.Sprintf("%s/api/asset?limit=%d&cursor=%s&allUsers=%v&all=%v", lapi.chosenServer, opts.Limit, opts.Cursor, opts.AllUsers, opts.IncludeDeleted)
	if len(opts.Filters) > 0 {
		filtersStrs := make([]string, 0, len(opts.Filters))
		for k, v := range opts.Filters {
			vb, err := json.Marshal(v)
			if err != nil {
				return nil, "", fmt.Errorf("error marshaling filter: %w", err)
			}
			filtersStrs = append(filtersStrs, fmt.Sprintf(`{"id":%q,"value":%s}`, k, vb))
		}
		url += fmt.Sprintf(`&filters=[%s]`, strings.Join(filtersStrs, ","))
	}
	if len(opts.Order) > 0 {
		orderStrs := make([]string, 0, len(opts.Order))
		for field, descending := range opts.Order {
			orderStrs = append(orderStrs, fmt.Sprintf(`%s-%v`, field, descending))
		}
		url += fmt.Sprintf(`&order=%s`, strings.Join(orderStrs, ","))
	}

	var assets []*Asset
	headers, err := lapi.doRequestHeaders("GET", url, "asset", "list-assets", nil, &assets)
	if err != nil {
		return nil, "", err
	}
	next := ""
	links := linkheader.ParseMultiple(headers["Link"]).FilterByRel("next")
	if len(links) > 0 {
		next = links[0].URL
	}
	return assets, next, nil
}

func (lapi *Client) GetAssetByPlaybackID(pid string, includeDeleted bool) (*Asset, error) {
	assets, _, err := lapi.ListAssets(ListOptions{
		Limit:          2,
		AllUsers:       true,
		IncludeDeleted: includeDeleted,
		Filters: map[string]interface{}{
			"playbackId": pid,
		}})
	if err != nil {
		return nil, err
	}
	if len(assets) == 0 {
		return nil, ErrNotExists
	} else if len(assets) > 1 {
		return nil, fmt.Errorf("multiple assets found for playbackId %q", pid)
	}
	return assets[0], nil
}

func (lapi *Client) DeleteAsset(id string) error {
	url := fmt.Sprintf("%s/api/asset/%s", lapi.chosenServer, id)
	return lapi.doRequest("DELETE", url, "asset", "", nil, nil)
}

func (lapi *Client) GetObjectStore(id string) (*ObjectStore, error) {
	var os ObjectStore
	url := fmt.Sprintf("%s/api/object-store/%s", lapi.chosenServer, id)
	if err := lapi.getJSON(url, "object_store", "", &os); err != nil {
		return nil, err
	}
	return &os, nil
}

func (lapi *Client) newRequest(method, url string, bodyObj interface{}) (*http.Request, error) {
	var body io.Reader
	if bodyObj != nil {
		b, err := json.Marshal(bodyObj)
		if err != nil {
			return nil, fmt.Errorf("error marshalling body: %w", err)
		}
		body = bytes.NewBuffer(b)
	}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", "Bearer "+lapi.accessToken)
	if body != nil {
		req.Header.Add("Content-Type", "application/json")
	}
	if lapi.userAgent != "" {
		req.Header.Add("User-Agent", lapi.userAgent)
	}
	return req, err
}

func (lapi *Client) getJSON(url, resourceType, metricName string, output interface{}) error {
	return lapi.doRequest("GET", url, resourceType, metricName, nil, output)
}

func (lapi *Client) doRequest(method, url, resourceType, metricName string, input, output interface{}) error {
	_, err := lapi.doRequestHeaders(method, url, resourceType, metricName, input, output)
	return err
}

// Does a request with retries
func (lapi *Client) doRequestHeaders(method, url, resourceType, metricName string, input, output interface{}) (http.Header, error) {
	if metricName == "" {
		metricName = strings.ToLower(method + "_" + resourceType)
	}
	var headers http.Header
	err := doWithRetries(metricName, 3, isRetriable, func() (err error) {
		headers, err = lapi.doRequestHeadersOnce(method, url, resourceType, metricName, input, output)
		return err
	})
	if err != nil {
		glog.Errorf("Post-retries error making request method=%s url=%q err=%q retriable=%v", method, url, err, isRetriable(err))
		return nil, err
	}
	return headers, nil
}

func (lapi *Client) doRequestHeadersOnce(method, url, resourceType, metricName string, input, output interface{}) (http.Header, error) {
	start := time.Now()
	req, err := lapi.newRequest(method, url, input)
	if err != nil {
		return nil, err
	}

	resp, err := lapi.httpClient.Do(req)
	if err != nil {
		glog.Errorf("Error calling Livepeer API resource=%s method=%s url=%s error=%q", resourceType, method, url, err)
		lapi.metrics.APIRequest(metricName, 0, err)
		return nil, err
	}
	defer resp.Body.Close()

	if err := checkResponseError(resp); err != nil {
		lapi.metrics.APIRequest(metricName, 0, err)
		return resp.Header, err
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Error reading Livepeer API response body resource=%s method=%s url=%s error=%q", resourceType, method, url, err)
		lapi.metrics.APIRequest(metricName, 0, err)
		return resp.Header, err
	}
	took := time.Since(start)
	lapi.metrics.APIRequest(metricName, took, nil)

	if output == nil {
		return resp.Header, nil
	}
	return resp.Header, json.Unmarshal(b, output)
}

// PushSegment pushes a segment with retries
func (lapi *Client) PushSegment(sid string, seqNo int, dur time.Duration, segData []byte, resolution string) ([][]byte, error) {
	shouldRetry := func(err error) bool {
		errMsg := strings.ToLower(err.Error())
		return isRetriable(err) ||
			strings.Contains(errMsg, "could not create stream id")
	}
	var transcoded [][]byte
	err := doWithRetries("push_segment", 6, shouldRetry, func() (err error) {
		transcoded, err = lapi.pushSegmentOnce(sid, seqNo, dur, segData, resolution)
		return
	})
	return transcoded, err
}

func (lapi *Client) pushSegmentOnce(sid string, seqNo int, dur time.Duration, segData []byte, resolution string) ([][]byte, error) {
	var err error
	if len(lapi.broadcasters) == 0 {
		lapi.broadcasters, err = lapi.Broadcasters()
		if err != nil {
			return nil, err
		}
		if len(lapi.broadcasters) == 0 {
			return nil, fmt.Errorf("no broadcasters available")
		}
	}
	timeout := 3*time.Second + 3*dur
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	urlToUp := fmt.Sprintf("%s/live/%s/%d.ts", lapi.broadcasters[0], sid, seqNo)
	body := bytes.NewReader(segData)
	req, err := http.NewRequestWithContext(ctx, "POST", urlToUp, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "multipart/mixed")
	req.Header.Set("Content-Duration", strconv.FormatInt(dur.Milliseconds(), 10))
	if resolution != "" {
		req.Header.Set("Content-Resolution", resolution)
	}

	postStarted := time.Now()
	resp, err := longTimeoutHTTPClient.Do(req)
	postTook := time.Since(postStarted)
	var status string
	if resp != nil {
		status = resp.Status
		defer resp.Body.Close()
	}
	glog.V(DEBUG).Infof("Post segment manifest=%s seqNo=%d dur=%s took=%s timed_out=%v status='%v' err=%v",
		sid, seqNo, dur, postTook, isTimeout(err), status, err)
	if err != nil {
		return nil, err
	}
	glog.V(VERBOSE).Infof("Got transcoded manifest=%s seqNo=%d resp status=%s reading body started", sid, seqNo, resp.Status)
	if err := checkResponseError(resp); err != nil {
		glog.V(DEBUG).Infof("Got manifest=%s seqNo=%d resp status=%s error=%+v", sid, seqNo, resp.Status, err)
		return nil, fmt.Errorf("transcode error: %w", err)
	}
	started := time.Now()
	mediaType, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		err = fmt.Errorf("error getting mime type manifestID=%s err=%w", sid, err)
		glog.Error(err)
		return nil, err
	}
	glog.V(VERBOSE).Infof("mediaType=%s params=%+v", mediaType, params)
	if glog.V(VVERBOSE) {
		for k, v := range resp.Header {
			glog.Infof("Header '%s': '%s'", k, v)
		}
	}
	var segments [][]byte
	var urls []string
	if mediaType == "multipart/mixed" {
		mr := multipart.NewReader(resp.Body, params["boundary"])
		for {
			p, merr := mr.NextPart()
			if merr == io.EOF {
				break
			}
			if merr != nil {
				glog.Error("Could not process multipart part ", merr, sid)
				err = merr
				break
			}
			mediaType, _, err := mime.ParseMediaType(p.Header.Get("Content-Type"))
			if err != nil {
				glog.Error("Error getting mime type ", err, sid)
				for k, v := range p.Header {
					glog.Infof("Header '%s': '%s'", k, v)
				}
			}
			body, merr := io.ReadAll(p)
			if merr != nil {
				glog.Errorf("error reading body manifest=%s seqNo=%d err=%v", sid, seqNo, merr)
				err = merr
				break
			}
			if mediaType == "application/vnd+livepeer.uri" {
				urls = append(urls, string(body))
			} else {
				var v glog.Level = DEBUG
				if len(body) < 5 {
					v = 0
				}
				glog.V(v).Infof("Read back segment for manifest=%s seqNo=%d profile=%d len=%d bytes", sid, seqNo, len(segments), len(body))
				segments = append(segments, body)
			}
		}
	}
	took := time.Since(started)
	glog.V(VERBOSE).Infof("Reading body back for manifest=%s seqNo=%d took=%s profiles=%d", sid, seqNo, took, len(segments))
	// glog.Infof("Body: %s", string(tbody))

	if err != nil {
		httpErr := fmt.Errorf(`error reading http request body for manifest=%s seqNo=%d err=%w`, sid, seqNo, err)
		glog.Error(httpErr)
		return nil, err
	}
	return segments, nil
}

// TranscodeFile transcodes a file
func (lapi *Client) TranscodeFile(tfr TranscodeFileReq) (*Task, error) {
	u := fmt.Sprintf("%s/api/transcode", lapi.chosenServer)
	var task *Task
	err := lapi.doRequest("POST", u, "task", "", tfr, &task)
	return task, err
}

// Calls the action function until no error or an unretriable error is returned,
// or the maximum number of tries is reached. Retriability is determined by the
// provided shouldRetry function.
func doWithRetries(apiName string, maxTries int, shouldRetry func(error) bool, action func() error) (err error) {
	backoff := 1 * time.Second
	for try := 1; try <= maxTries; try++ {
		err = action()
		if err == nil || !shouldRetry(err) {
			return
		}
		if try < maxTries {
			glog.Infof("Retrying API due to error after backoff=%v api=%s try=%d err=%q", backoff, apiName, try, err)
			time.Sleep(backoff)
			backoff *= 2
		}
	}
	return
}

func isRetriable(err error) bool {
	if err == nil {
		return false
	}
	return isTimeout(err) ||
		isTemporaryNetErr(err) ||
		strings.HasPrefix(err.Error(), "request failed with status 5") // 5xx status code
}

func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	var terr interface {
		Timeout() bool
	}
	if errors.As(err, &terr) && terr.Timeout() {
		return true
	}
	errMsg := strings.ToLower(err.Error())
	return errors.Is(err, syscall.ETIMEDOUT) ||
		strings.Contains(errMsg, "client.timeout") ||
		strings.Contains(errMsg, "context canceled") ||
		strings.Contains(errMsg, "context deadline exceeded")
}

func isTemporaryNetErr(err error) bool {
	if err == nil {
		return false
	}
	var terr interface {
		Temporary() bool
	}
	if errors.As(err, &terr) && terr.Temporary() {
		return true
	}
	errMsg := strings.ToLower(err.Error())
	return errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.ENETRESET) ||
		errors.Is(err, syscall.EPIPE) ||
		strings.Contains(errMsg, "connection timed out") ||
		strings.Contains(errMsg, "network is down")
}

func checkResponseError(resp *http.Response) error {
	if isSuccessStatus(resp.StatusCode) {
		return nil
	}
	body, err := io.ReadAll(resp.Body)
	glog.V(logs.VERBOSE).Infof("Status error from Livepeer API method=%s url=%s status=%d body=%q", resp.Request.Method, resp.Request.URL, resp.StatusCode, string(body))
	if err != nil {
		return fmt.Errorf("failed reading error response (%s): %w", resp.Status, err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return ErrNotExists
	} else if resp.StatusCode == http.StatusTooManyRequests {
		return ErrRateLimited
	}
	var errResp struct {
		Errors []string `json:"errors"`
	}
	if err := json.Unmarshal(body, &errResp); err != nil || len(errResp.Errors) == 0 {
		return fmt.Errorf("request failed with status %s and body: %s", resp.Status, body)
	}
	return fmt.Errorf("request failed with status %s and errors: %v", resp.Status, errResp.Errors)
}

func isSuccessStatus(status int) bool {
	return status >= 200 && status < 300
}
