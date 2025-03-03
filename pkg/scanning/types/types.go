package types

import "encoding/json"

const (
	Version = iota
	V1
	V2
)

type Scan struct {
	Ip          string `json:"ip"`
	Port        uint32 `json:"port"`
	Service     string `json:"service"`
	Timestamp   int64  `json:"timestamp"`
	DataVersion int    `json:"data_version"`
	Data        string `json:"data"`
}

// This was something I was considering for respose as it contains the Version of the data as well as the DataVersion
type ScanResponse struct {
	Ip          string `json:"ip"`
	Port        uint32 `json:"port"`
	Service     string `json:"service"`
	Timestamp   int64  `json:"timestamp"`
	DataVersion int    `json:"data_version"`
	Data        string `json:"data"`
	Version     int    `json:"version"`
}

type V1Data struct {
	ResponseBytesUtf8 []byte `json:"response_bytes_utf8"`
}

type V2Data struct {
	ResponseStr string `json:"response_str"`
}

// TempScan is used for initial unmarshaling so we can inspect DataVersion and decode Data appropriately.
type TempScan struct {
	Ip          string          `json:"ip"`
	Port        uint32          `json:"port"`
	Service     string          `json:"service"`
	Timestamp   int64           `json:"timestamp"`
	DataVersion int             `json:"data_version"`
	Data        json.RawMessage `json:"data"`
}
