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
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/go-api-client/logs"
	"github.com/livepeer/go-api-client/metrics"
)

// ErrNotExists returned if stream is not found
var ErrNotExists = errors.New("stream does not exist")

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
	}

	geoResp struct {
		ChosenServer string `json:"chosenServer,omitempty"`
		Servers      []struct {
			Server   string `json:"server,omitempty"`
			Duration int    `json:"duration,omitempty"`
		} `json:"servers,omitempty"`
	}

	createStreamReq struct {
		Name    string   `json:"name,omitempty"`
		Presets []string `json:"presets,omitempty"`
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
		Profiles []Profile `json:"profiles,omitempty"`
		Record   bool      `json:"record,omitempty"`
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
	}

	MultistreamTargetRef struct {
		Profile   string `json:"profile,omitempty"`
		VideoOnly bool   `json:"videoOnly,omitempty"`
		ID        string `json:"id,omitempty"`
	}

	// CreateStreamResp returned by API
	CreateStreamResp struct {
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
		Errors                     []string  `json:"errors,omitempty"`
		Multistream                struct {
			Targets []MultistreamTargetRef `json:"targets,omitempty"`
		} `json:"multistream"`
	}

	// UserSession user's sessions
	UserSession struct {
		CreateStreamResp
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
		ParentAssetID string `json:"parentAssetId"`
		Type          string `json:"type"`
		Params        struct {
			Import *struct {
				URL string `json:"url"`
			} `json:"import"`
		} `json:"params"`
	}

	Asset struct {
		ID            string `json:"id"`
		UserID        string `json:"userId"`
		CreatedAt     int64  `json:"createdAt"`
		ObjectStoreID string `json:"objectStoreId"`
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

// CreateStream creates stream with specified name and profiles
func (lapi *Client) CreateStream(name string, presets ...string) (string, error) {
	csr, err := lapi.CreateStreamEx(name, false, presets)
	if err != nil {
		return "", err
	}
	return csr.ID, err
}

// DeleteStream deletes stream
func (lapi *Client) DeleteStream(id string) error {
	glog.V(logs.DEBUG).Infof("Deleting Livepeer stream '%s' ", id)
	u := fmt.Sprintf("%s/api/stream/%s", lapi.chosenServer, id)
	req, err := lapi.newRequest("DELETE", u, nil)
	if err != nil {
		return err
	}
	req.Header.Add("Authorization", "Bearer "+lapi.accessToken)
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
func (lapi *Client) CreateStreamEx(name string, record bool, presets []string, profiles ...Profile) (*CreateStreamResp, error) {
	return lapi.CreateStreamEx2(name, record, "", presets, profiles...)
}

// CreateStreamEx creates stream with specified name and profiles
func (lapi *Client) CreateStreamEx2(name string, record bool, parentID string, presets []string, profiles ...Profile) (*CreateStreamResp, error) {
	// presets := profiles
	// if len(presets) == 0 {
	// 	presets = lapi.presets
	// }
	glog.Infof("Creating Livepeer stream '%s' with presets '%v' and profiles %+v", name, presets, profiles)
	reqs := &createStreamReq{
		Name:    name,
		Presets: presets,
		Record:  record,
	}
	if len(presets) == 0 {
		reqs.Profiles = StandardProfiles
	}
	if len(profiles) > 0 {
		reqs.Profiles = profiles
	}
	b, err := json.Marshal(reqs)
	if err != nil {
		glog.V(logs.SHORT).Infof("Error marshalling create stream request %v", err)
		return nil, err
	}
	glog.Infof("Sending: %s", b)
	u := fmt.Sprintf("%s/api/stream", lapi.chosenServer)
	if parentID != "" {
		u = fmt.Sprintf("%s/api/stream/%s/stream", lapi.chosenServer, parentID)
	}
	req, err := lapi.newRequest("POST", u, bytes.NewBuffer(b))
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", "Bearer "+lapi.accessToken)
	req.Header.Add("Content-Type", "application/json")
	resp, err := lapi.httpClient.Do(req)
	if err != nil {
		glog.Errorf("Error creating Livepeer stream %v", err)
		return nil, err
	}
	defer resp.Body.Close()
	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Error creating Livepeer stream (body) %v", err)
		return nil, err
	}
	glog.Info(string(b))
	r := &CreateStreamResp{}
	err = json.Unmarshal(b, r)
	if err != nil {
		return nil, err
	}
	if len(r.Errors) > 0 {
		return nil, fmt.Errorf("error creating stream: %+v", r.Errors)
	}
	glog.Infof("Stream %s created with id %s", name, r.ID)
	return r, nil
}

// DefaultPresets returns default presets
func (lapi *Client) DefaultPresets() []string {
	return lapi.presets
}

// GetStreamByKey gets stream by streamKey
func (lapi *Client) GetStreamByKey(key string) (*CreateStreamResp, error) {
	if key == "" {
		return nil, errors.New("empty key")
	}
	u := fmt.Sprintf("%s/api/stream/key/%s?main=true", lapi.chosenServer, key)
	return lapi.getStream(u, "get_by_key")
}

// GetStreamByPlaybackID gets stream by playbackID
func (lapi *Client) GetStreamByPlaybackID(playbackID string) (*CreateStreamResp, error) {
	if playbackID == "" {
		return nil, errors.New("empty playbackID")
	}
	u := fmt.Sprintf("%s/api/stream/playback/%s", lapi.chosenServer, playbackID)
	return lapi.getStream(u, "get_by_playbackid")
}

// GetStream gets stream by id
func (lapi *Client) GetStream(id string) (*CreateStreamResp, error) {
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
	req.Header.Add("Authorization", "Bearer "+lapi.accessToken)
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
	req.Header.Add("Authorization", "Bearer "+lapi.accessToken)
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
	b, _ := json.Marshal(&ar)
	req, err := lapi.newRequest("PUT", u, bytes.NewBuffer(b))
	if err != nil {
		lapi.metrics.APIRequest("set_active", 0, err)
		return true, err
	}
	ctx, cancel := context.WithTimeout(req.Context(), setActiveTimeout)
	defer cancel()
	req = req.WithContext(ctx)

	req.Header.Add("Authorization", "Bearer "+lapi.accessToken)
	req.Header.Add("Content-Type", "application/json")
	resp, err := lapi.httpClient.Do(req)
	if err != nil {
		glog.Errorf("id=%s/setactive Error set active %v", id, err)
		lapi.metrics.APIRequest("set_active", 0, err)
		return true, err
	}
	defer resp.Body.Close()
	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("id=%s/setactive Error set active (body) %v", err)
		lapi.metrics.APIRequest("set_active", 0, err)
		return true, err
	}
	took := time.Since(start)
	lapi.metrics.APIRequest("set_active", took, nil)
	glog.Infof("%s/setactive took=%s response status code %d status %s resp %+v body=%s",
		id, took, resp.StatusCode, resp.Status, resp, string(b))
	return resp.StatusCode >= 200 && resp.StatusCode < 300, nil
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
	b, _ := json.Marshal(&dmreq)
	req, err := lapi.newRequest("PATCH", u, bytes.NewBuffer(b))
	if err != nil {
		lapi.metrics.APIRequest("deactivate-many", 0, err)
		return 0, err
	}
	req.Header.Add("Authorization", "Bearer "+lapi.accessToken)
	req.Header.Add("Content-Type", "application/json")
	resp, err := lapi.httpClient.Do(req)
	if err != nil {
		glog.Errorf("/deactivate-many err=%v", err)
		lapi.metrics.APIRequest("set_active", 0, err)
		return 0, err
	}
	defer resp.Body.Close()
	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("deactivate-many body err=%v", err)
		lapi.metrics.APIRequest("deactivate-many", 0, err)
		return 0, err
	}
	took := time.Since(start)
	lapi.metrics.APIRequest("deactivate-many", took, nil)
	glog.Infof("deactivate-many took=%s response status code %d status %s resp %+v body=%s",
		took, resp.StatusCode, resp.Status, resp, string(b))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("invalid status code: %d", resp.StatusCode)
	}

	mr := &deactivateManyResp{}
	err = json.Unmarshal(b, mr)
	if err != nil {
		return 0, err
	}

	return mr.RowCount, nil
}

func (lapi *Client) getStream(url, metricName string) (*CreateStreamResp, error) {
	var stream *CreateStreamResp
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

func (lapi *Client) newRequest(method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
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
	if metricName == "" {
		metricName = "get_" + resourceType
	}
	start := time.Now()
	req := lapi.getRequest(url)
	req.Header.Add("Authorization", "Bearer "+lapi.accessToken)

	resp, err := lapi.httpClient.Do(req)
	if err != nil {
		glog.Errorf("Error getting object from Livepeer API resource=%s url=%s error=%q", resourceType, url, err)
		lapi.metrics.APIRequest(metricName, 0, err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		glog.Errorf("Status error from Livepeer API resource=%s url=%s status=%d body=%q", resourceType, url, resp.StatusCode, string(b))
		if resp.StatusCode == http.StatusNotFound {
			lapi.metrics.APIRequest(metricName, 0, ErrNotExists)
			return ErrNotExists
		}
		err := errors.New(http.StatusText(resp.StatusCode))
		lapi.metrics.APIRequest(metricName, 0, err)
		return err
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Error reading Livepeer API response body resource=%s url=%s error=%q", resourceType, url, err)
		lapi.metrics.APIRequest(metricName, 0, err)
		return err
	}
	took := time.Since(start)
	lapi.metrics.APIRequest(metricName, took, nil)

	return json.Unmarshal(b, output)
}
