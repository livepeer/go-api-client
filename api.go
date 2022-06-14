// Package livepeer API
package livepeerAPI

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/go-api-client/logs"
	"github.com/livepeer/go-api-client/metrics"
)

const (
	SHORT    = 4
	DEBUG    = 5
	VERBOSE  = 6
	VVERBOSE = 7
	INSANE   = 12
	INSANE2  = 14
)

// ErrNotExists returned if receives a 404 error from the API
var ErrNotExists = errors.New("not found")

const httpTimeout = 4 * time.Second
const setActiveTimeout = 1500 * time.Millisecond

var defaultHTTPClient = &http.Client{
	Timeout: httpTimeout,
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
	}

	// UserSession user's sessions
	UserSession struct {
		Stream
		RecordingStatus string `json:"recordingStatus,omitempty"` // ready, waiting
		RecordingURL    string `json:"recordingUrl,omitempty"`
		Mp4Url          string `json:"mp4Url,omitempty"`
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
			Import    *ImportTaskParams    `json:"import"`
			Export    *ExportTaskParams    `json:"export"`
			Transcode *TranscodeTaskParams `json:"transcode"`
		} `json:"params"`
		Status TaskStatus `json:"status"`
	}

	TaskStatus struct {
		Phase     string  `json:"phase"`
		Progress  float64 `json:"progress"`
		UpdatedAt int64   `json:"updatedAt,omitempty"`
	}

	ImportTaskParams struct {
		URL               string `json:"url,omitempty"`
		UploadedObjectKey string `json:"uploadedObjectKey,omitempty"`
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

	updateTaskProgressRequest struct {
		Status TaskStatus `json:"status"`
	}

	Asset struct {
		ID            string `json:"id"`
		PlaybackID    string `json:"playbackId"`
		UserID        string `json:"userId"`
		CreatedAt     int64  `json:"createdAt"`
		SourceAssetId string `json:"sourceAssetId,omitempty"`
		ObjectStoreID string `json:"objectStoreId"`
		AssetSpec
	}

	AssetSpec struct {
		Name                string          `json:"name,omitempty"`
		Type                string          `json:"type"`
		Size                uint64          `json:"size"`
		Hash                []AssetHash     `json:"hash"`
		VideoSpec           *AssetVideoSpec `json:"videoSpec,omitempty"`
		PlaybackRecordingID string          `json:"playbackRecordingId,omitempty"`
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
		b, _ := ioutil.ReadAll(resp.Body)
		return "", fmt.Errorf("status error contacting Livepeer API server (%s) status %d body: %s", livepeerAPIGeolocateURL, resp.StatusCode, string(b))
	}
	b, err := ioutil.ReadAll(resp.Body)
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
	resp, err := lapi.httpClient.Do(lapi.getRequest(u))
	if err != nil {
		glog.Errorf("Error getting broadcasters from Livepeer API server (%s) error: %v", u, err)
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		glog.Fatalf("Status error contacting Livepeer API server (%s) status %d body: %s", livepeerAPIGeolocateURL, resp.StatusCode, string(b))
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Fatalf("Error geolocating Livepeer API server (%s) error: %v", livepeerAPIGeolocateURL, err)
	}
	glog.Info(string(b))
	broadcasters := []addressResp{}
	err = json.Unmarshal(b, &broadcasters)
	if err != nil {
		return nil, err
	}
	bs := make([]string, 0, len(broadcasters))
	for _, a := range broadcasters {
		bs = append(bs, a.Address)
	}
	return bs, nil
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
	resp, err := lapi.httpClient.Do(lapi.getRequest(u))
	if err != nil {
		glog.Errorf("Error getting ingests from Livepeer API server (%s) error: %v", u, err)
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		glog.Fatalf("Status error contacting Livepeer API server (%s) status %d body: %s", lapi.chosenServer, resp.StatusCode, string(b))
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Fatalf("Error reading from Livepeer API server (%s) error: %v", lapi.chosenServer, err)
	}
	glog.Info(string(b))
	ingests := []Ingest{}
	err = json.Unmarshal(b, &ingests)
	if err != nil {
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
	req, err := lapi.newRequest("DELETE", u, nil)
	if err != nil {
		return err
	}
	resp, err := lapi.httpClient.Do(req)
	if err != nil {
		glog.Errorf("Error deleting Livepeer stream %v", err)
		return err
	}
	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Error deleting Livepeer stream (body) %v", err)
		return err
	}
	if resp.StatusCode != 204 {
		return fmt.Errorf("error deleting stream %s: status is %s", id, resp.Status)
	}
	return nil
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
	req, err := lapi.newRequest("POST", u, csr)
	if err != nil {
		return nil, err
	}
	httpResp, err := lapi.httpClient.Do(req)
	if err != nil {
		glog.Errorf("Error creating stream err=%+v", err)
		return nil, err
	}
	defer httpResp.Body.Close()

	if err := checkResponseError(httpResp); err != nil {
		return nil, fmt.Errorf("error creating stream: %w", err)
	}
	var stream *Stream
	if err = json.NewDecoder(httpResp.Body).Decode(&stream); err != nil {
		return nil, fmt.Errorf("error parsing create stream response: %w", err)
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
func (lapi *Client) GetStream(id string) (*Stream, error) {
	if id == "" {
		return nil, errors.New("empty id")
	}
	u := fmt.Sprintf("%s/api/stream/%s", lapi.chosenServer, id)
	return lapi.getStream(u, "get_by_id")
}

// GetSessionsR gets user's sessions for the stream by id
func (lapi *Client) GetSessionsR(id string, forceUrl bool) ([]UserSession, error) {
	var apiTry int
	for {
		sessions, err := lapi.GetSessions(id, forceUrl)
		if err != nil {
			if Timedout(err) && apiTry < 3 {
				apiTry++
				continue
			}
		}
		return sessions, err
	}
}

// GetSessionsNewR gets user's sessions for the stream by id
func (lapi *Client) GetSessionsNewR(id string, forceUrl bool) ([]UserSession, error) {
	var apiTry int
	for {
		sessions, err := lapi.GetSessionsNew(id, forceUrl)
		if err != nil {
			if Timedout(err) && apiTry < 3 {
				apiTry++
				continue
			}
		}
		return sessions, err
	}
}

func (lapi *Client) GetSessionsNew(id string, forceUrl bool) ([]UserSession, error) {
	if id == "" {
		return nil, errors.New("empty id")
	}
	u := fmt.Sprintf("%s/api/session?parentId=%s", lapi.chosenServer, id)
	if forceUrl {
		u += "&forceUrl=1"
	}
	start := time.Now()
	req := lapi.getRequest(u)
	resp, err := lapi.httpClient.Do(req)
	if err != nil {
		glog.Errorf("Error getting sessions for stream by id from Livepeer API server (%s) error: %v", u, err)
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		glog.Errorf("Status error getting sessions for stream by id Livepeer API server (%s) status %d body: %s", u, resp.StatusCode, string(b))
		if resp.StatusCode == http.StatusNotFound {
			return nil, ErrNotExists
		}
		err := errors.New(http.StatusText(resp.StatusCode))
		return nil, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Error getting sessions for stream by id Livepeer API server (%s) error: %v", u, err)
		return nil, err
	}
	took := time.Since(start)
	glog.V(logs.DEBUG).Infof("sessions request for id=%s took=%s", id, took)
	bs := string(b)
	glog.Info(bs)
	glog.V(logs.VERBOSE).Info(bs)
	if bs == "null" || bs == "" {
		// API return null if stream does not exists
		return nil, ErrNotExists
	}
	r := []UserSession{}
	err = json.Unmarshal(b, &r)
	if err != nil {
		return nil, err
	}
	return r, nil
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
	start := time.Now()
	req := lapi.getRequest(u)
	resp, err := lapi.httpClient.Do(req)
	if err != nil {
		glog.Errorf("Error getting sessions for stream by id from Livepeer API server (%s) error: %v", u, err)
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		glog.Errorf("Status error getting sessions for stream by id Livepeer API server (%s) status %d body: %s", u, resp.StatusCode, string(b))
		if resp.StatusCode == http.StatusNotFound {
			return nil, ErrNotExists
		}
		err := errors.New(http.StatusText(resp.StatusCode))
		return nil, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Error getting sessions for stream by id Livepeer API server (%s) error: %v", u, err)
		return nil, err
	}
	took := time.Since(start)
	glog.V(logs.DEBUG).Infof("sessions request for id=%s took=%s", id, took)
	bs := string(b)
	glog.V(logs.VERBOSE).Info(bs)
	if bs == "null" || bs == "" {
		// API return null if stream does not exists
		return nil, ErrNotExists
	}
	r := []UserSession{}
	err = json.Unmarshal(b, &r)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// SetActiveR sets stream active with retries
func (lapi *Client) SetActiveR(id string, active bool, startedAt time.Time) (bool, error) {
	apiTry := 1
	for {
		ok, err := lapi.SetActive(id, active, startedAt)
		if err != nil {
			if Timedout(err) && apiTry < 3 {
				apiTry++
				continue
			}
			glog.Errorf("Fatal error calling API /setactive id=%s active=%s err=%v", id, active, err)
		}
		return ok, err
	}
}

// SetActive set isActive
func (lapi *Client) SetActive(id string, active bool, startedAt time.Time) (bool, error) {
	if id == "" {
		return true, errors.New("empty id")
	}
	start := time.Now()
	u := fmt.Sprintf("%s/api/stream/%s/setactive", lapi.chosenServer, id)
	ar := setActiveReq{
		Active:   active,
		HostName: hostName,
	}
	if !startedAt.IsZero() {
		ar.StartedAt = startedAt.UnixNano() / int64(time.Millisecond)
	}
	req, err := lapi.newRequest("PUT", u, &ar)
	if err != nil {
		lapi.metrics.APIRequest("set_active", 0, err)
		return true, err
	}
	ctx, cancel := context.WithTimeout(req.Context(), setActiveTimeout)
	defer cancel()
	req = req.WithContext(ctx)

	resp, err := lapi.httpClient.Do(req)
	if err != nil {
		glog.Errorf("id=%s/setactive Error set active %v", id, err)
		lapi.metrics.APIRequest("set_active", 0, err)
		return true, err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("id=%s/setactive Error set active (body) %v", err)
		lapi.metrics.APIRequest("set_active", 0, err)
		return true, err
	}
	took := time.Since(start)
	lapi.metrics.APIRequest("set_active", took, nil)
	glog.Infof("%s/setactive took=%s response status code %d status %s resp %+v body=%s",
		id, took, resp.StatusCode, resp.Status, resp, string(b))
	return isSuccessStatus(resp.StatusCode), nil
}

// DeactivateMany sets many streams isActive field to false
func (lapi *Client) DeactivateMany(ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, errors.New("empty ids")
	}
	start := time.Now()
	u := fmt.Sprintf("%s/api/stream/deactivate-many", lapi.chosenServer)
	dmreq := deactivateManyReq{
		IDS: ids,
	}
	req, err := lapi.newRequest("PATCH", u, &dmreq)
	if err != nil {
		lapi.metrics.APIRequest("deactivate-many", 0, err)
		return 0, err
	}
	resp, err := lapi.httpClient.Do(req)
	if err != nil {
		glog.Errorf("/deactivate-many err=%v", err)
		lapi.metrics.APIRequest("set_active", 0, err)
		return 0, err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("deactivate-many body err=%v", err)
		lapi.metrics.APIRequest("deactivate-many", 0, err)
		return 0, err
	}
	took := time.Since(start)
	lapi.metrics.APIRequest("deactivate-many", took, nil)
	glog.Infof("deactivate-many took=%s response status code %d status %s resp %+v body=%s",
		took, resp.StatusCode, resp.Status, resp, string(b))
	if !isSuccessStatus(resp.StatusCode) {
		return 0, fmt.Errorf("invalid status code: %d", resp.StatusCode)
	}

	mr := &deactivateManyResp{}
	err = json.Unmarshal(b, mr)
	if err != nil {
		return 0, err
	}

	return mr.RowCount, nil
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

func (lapi *Client) GetTask(id string) (*Task, error) {
	var task Task
	url := fmt.Sprintf("%s/api/task/%s", lapi.chosenServer, id)
	if err := lapi.getJSON(url, "task", "", &task); err != nil {
		return nil, err
	}
	return &task, nil
}

func (lapi *Client) UpdateTaskStatus(id string, phase string, progress float64) error {
	var (
		url    = fmt.Sprintf("%s/api/task/%s/status", lapi.chosenServer, id)
		input  = &updateTaskProgressRequest{Status: TaskStatus{phase, progress, 0}}
		output json.RawMessage
	)
	err := lapi.doRequest("POST", url, "task", "update_task_progress", input, &output)
	if err != nil {
		return err
	}
	glog.V(logs.DEBUG).Infof("Updated task progress id=%s phase=%s progress=%v output=%q", id, phase, progress, string(output))
	return nil
}

func (lapi *Client) GetMultistreamTarget(id string) (*MultistreamTarget, error) {
	var target MultistreamTarget
	url := fmt.Sprintf("%s/api/multistream/target/%s", lapi.chosenServer, id)
	if err := lapi.getJSON(url, "multistream_target", "", &target); err != nil {
		return nil, err
	}
	return &target, nil
}

// GetMultistreamTargetR gets multistream target with retries
func (lapi *Client) GetMultistreamTargetR(id string) (*MultistreamTarget, error) {
	var apiTry int
	for {
		target, err := lapi.GetMultistreamTarget(id)
		if err != nil {
			if Timedout(err) && apiTry < 3 {
				apiTry++
				continue
			}
		}
		return target, err
	}
}

func (lapi *Client) GetAsset(id string) (*Asset, error) {
	var asset Asset
	url := fmt.Sprintf("%s/api/asset/%s", lapi.chosenServer, id)
	if err := lapi.getJSON(url, "asset", "", &asset); err != nil {
		return nil, err
	}
	return &asset, nil
}

func (lapi *Client) GetObjectStore(id string) (*ObjectStore, error) {
	var os ObjectStore
	url := fmt.Sprintf("%s/api/object-store/%s", lapi.chosenServer, id)
	if err := lapi.getJSON(url, "object_store", "", &os); err != nil {
		return nil, err
	}
	return &os, nil
}

func Timedout(e error) bool {
	t, ok := e.(interface {
		Timeout() bool
	})
	return ok && t.Timeout() || (e != nil && strings.Contains(e.Error(), "Client.Timeout"))
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

func (lapi *Client) getRequest(url string) *http.Request {
	req, err := lapi.newRequest("GET", url, nil)
	if err != nil {
		glog.Fatal(err)
	}
	return req
}

func (lapi *Client) getJSON(url, resourceType, metricName string, output interface{}) error {
	return lapi.doRequest("GET", url, resourceType, metricName, nil, output)
}

func (lapi *Client) doRequest(method, url, resourceType, metricName string, input, output interface{}) error {
	if metricName == "" {
		metricName = strings.ToLower(method + "_" + resourceType)
	}
	start := time.Now()
	req, err := lapi.newRequest(method, url, input)
	if err != nil {
		return err
	}

	resp, err := lapi.httpClient.Do(req)
	if err != nil {
		glog.Errorf("Error calling Livepeer API resource=%s method=%s url=%s error=%q", resourceType, method, url, err)
		lapi.metrics.APIRequest(metricName, 0, err)
		return err
	}
	defer resp.Body.Close()

	if err := checkResponseError(resp); err != nil {
		lapi.metrics.APIRequest(metricName, 0, err)
		return err
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Error reading Livepeer API response body resource=%s method=%s url=%s error=%q", resourceType, method, url, err)
		lapi.metrics.APIRequest(metricName, 0, err)
		return err
	}
	took := time.Since(start)
	lapi.metrics.APIRequest(metricName, took, nil)

	return json.Unmarshal(b, output)
}

func (lapi *Client) PushSegment(sid string, seqNo int, dur time.Duration, segData []byte, resolution string) ([][]byte, error) {
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
	urlToUp := fmt.Sprintf("%s/live/%s/%d.ts", lapi.broadcasters[0], sid, seqNo)
	var body io.Reader
	body = bytes.NewReader(segData)
	req, err := http.NewRequest("POST", urlToUp, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "multipart/mixed")
	req.Header.Set("Content-Duration", strconv.FormatInt(dur.Milliseconds(), 10))
	if resolution != "" {
		req.Header.Set("Content-Resolution", resolution)
	}

	postStarted := time.Now()
	resp, err := lapi.httpClient.Do(req)
	postTook := time.Since(postStarted)
	var timedout bool
	var status string
	if err != nil {
		uerr := err.(*url.Error)
		timedout = uerr.Timeout()
	}
	if resp != nil {
		status = resp.Status
		defer resp.Body.Close()
	}
	glog.V(DEBUG).Infof("Post segment manifest=%s seqNo=%d dur=%s took=%s timed_out=%v status='%v' err=%v",
		sid, seqNo, dur, postTook, timedout, status, err)
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
			body, merr := ioutil.ReadAll(p)
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

func checkResponseError(resp *http.Response) error {
	if isSuccessStatus(resp.StatusCode) {
		return nil
	}
	body, err := ioutil.ReadAll(resp.Body)
	glog.Errorf("Status error from Livepeer API method=%s url=%s status=%d body=%q", resp.Request.Method, resp.Request.URL, resp.StatusCode, string(body))
	if err != nil {
		return fmt.Errorf("failed reading error response (%s): %w", resp.Status, err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return ErrNotExists
	}
	var errResp struct {
		Errors []string `json:"errors"`
	}
	if err := json.Unmarshal(body, &errResp); err != nil {
		return fmt.Errorf("request failed (%s) and failed parsing error response (%s): %w", resp.Status, body, err)
	}
	return fmt.Errorf("error response (%s) from api: %v", resp.Status, errResp.Errors)
}

func isSuccessStatus(status int) bool {
	return status >= 200 && status < 300
}
