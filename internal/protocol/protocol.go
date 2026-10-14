// Package protocol defines the JSON request/response types and error codes
// of the HTTP API. It is the only package shared by servers and clients.
//
// The wire format is intentionally separate from the storage format
// (internal/record): JSON values are strings for ease of use with curl,
// while stored records are length-prefixed binary frames. Replication does
// not use these types; followers fetch raw frames (see broker/replica.go).
package protocol

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// Error codes. Clients branch on these, never on message text.
const (
	CodeNotLeader          = "NOT_LEADER"
	CodeLeaseExpired       = "LEASE_EXPIRED"
	CodeNotEnoughReplicas  = "NOT_ENOUGH_REPLICAS"
	CodeReplicationTimeout = "REPLICATION_TIMEOUT"
	CodeOutOfOrderSequence = "OUT_OF_ORDER_SEQUENCE"
	CodeInvalidRecord      = "INVALID_RECORD"
	CodeInvalidRequest     = "INVALID_REQUEST"
	CodeOffsetOutOfRange   = "OFFSET_OUT_OF_RANGE"
	CodeUnknownTopic       = "UNKNOWN_TOPIC_OR_PARTITION"
	CodeTopicExists        = "TOPIC_ALREADY_EXISTS"
	CodeThrottled          = "THROTTLED"
	CodeOverloaded         = "BROKER_OVERLOADED"
	CodeStorage            = "STORAGE_ERROR"
	CodeUnauthenticated    = "UNAUTHENTICATED"
	CodeForbidden          = "FORBIDDEN"
	CodeFencedEpoch        = "FENCED_LEADER_EPOCH"
	CodeUnknownEpoch       = "UNKNOWN_LEADER_EPOCH"
	CodeUnknownReplica     = "UNKNOWN_REPLICA"
	CodeUnknownMember      = "UNKNOWN_MEMBER_ID"
	CodeIllegalGeneration  = "ILLEGAL_GENERATION"
	CodeRebalance          = "REBALANCE_IN_PROGRESS"
	CodeGroupNotEmpty      = "GROUP_NOT_EMPTY"
	CodeNotOwner           = "PARTITION_NOT_OWNED"
	CodeStaleISRUpdate     = "STALE_ISR_UPDATE"
	CodeInsufficientBroker = "INSUFFICIENT_BROKERS"
	CodeQuotaExceeded      = "QUOTA_EXCEEDED"
	CodeUnavailable        = "UNAVAILABLE"
	CodeInternal           = "INTERNAL"
)

// retriable lists codes a well-behaved client may retry (after refreshing
// metadata where relevant).
var retriable = map[string]bool{
	CodeNotLeader: true, CodeLeaseExpired: true, CodeNotEnoughReplicas: true,
	CodeReplicationTimeout: true, CodeThrottled: true, CodeOverloaded: true,
	CodeUnknownTopic: true, CodeFencedEpoch: true, CodeUnknownEpoch: true,
	CodeRebalance: true, CodeUnavailable: true, CodeStorage: true,
}

// IsRetriable reports whether code is transient.
func IsRetriable(code string) bool { return retriable[code] }

// Error is the body of every non-2xx response.
type Error struct {
	Code           string `json:"code"`
	Message        string `json:"message"`
	Retriable      bool   `json:"retriable"`
	Status         int    `json:"-"`
	LeaderID       *int32 `json:"leader_id,omitempty"`
	LeaderURL      string `json:"leader_url,omitempty"`
	RetryAfterMs   int64  `json:"retry_after_ms,omitempty"`
	LogStartOffset *int64 `json:"log_start_offset,omitempty"`
	HighWatermark  *int64 `json:"high_watermark,omitempty"`
}

func (e *Error) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("%s (%d): %s", e.Code, e.Status, e.Message)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Errorf builds an Error.
func Errorf(status int, code, format string, args ...any) *Error {
	return &Error{Status: status, Code: code, Message: fmt.Sprintf(format, args...), Retriable: IsRetriable(code)}
}

type errorEnvelope struct {
	Error *Error `json:"error"`
}

// WriteError writes e as JSON with its status code and Retry-After header.
func WriteError(w http.ResponseWriter, e *Error) {
	if e.Status == 0 {
		e.Status = http.StatusInternalServerError
	}
	e.Retriable = e.Retriable || IsRetriable(e.Code)
	if e.RetryAfterMs > 0 {
		w.Header().Set("Retry-After", fmt.Sprint((e.RetryAfterMs+999)/1000))
	}
	WriteJSON(w, e.Status, errorEnvelope{Error: e})
}

// WriteJSON writes v as JSON.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

// DecodeError parses an error response body.
func DecodeError(status int, body []byte) *Error {
	var env errorEnvelope
	if err := json.Unmarshal(body, &env); err != nil || env.Error == nil {
		code := CodeInternal
		if status == http.StatusServiceUnavailable || status == http.StatusBadGateway || status == http.StatusGatewayTimeout {
			code = CodeUnavailable
		}
		return &Error{Status: status, Code: code, Message: string(body), Retriable: IsRetriable(code)}
	}
	env.Error.Status = status
	return env.Error
}

// TopicPartition identifies a partition.
type TopicPartition struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
}

func (tp TopicPartition) String() string { return fmt.Sprintf("%s-%d", tp.Topic, tp.Partition) }

// --- produce / fetch -------------------------------------------------------

// Record is a record as sent by producers. A nil Value is a tombstone.
type Record struct {
	Key     *string           `json:"key,omitempty"`
	Value   *string           `json:"value"`
	Headers map[string]string `json:"headers,omitempty"`
	// Partition is only used by the gateway's topic-level endpoint to pin a
	// record to an explicit partition.
	Partition *int32 `json:"partition,omitempty"`
}

// ProduceRequest is a batch for one partition.
type ProduceRequest struct {
	ProducerID   int64    `json:"producer_id,omitempty"`
	BaseSequence int64    `json:"base_sequence,omitempty"`
	Acks         string   `json:"acks,omitempty"` // leader | all | quorum
	TimeoutMs    int      `json:"timeout_ms,omitempty"`
	Records      []Record `json:"records"`
}

// ProduceResponse describes where a batch landed.
type ProduceResponse struct {
	Topic         string `json:"topic"`
	Partition     int32  `json:"partition"`
	BaseOffset    int64  `json:"base_offset"`
	LastOffset    int64  `json:"last_offset"`
	Duplicate     bool   `json:"duplicate,omitempty"`
	LogAppendTime int64  `json:"log_append_time,omitempty"`
}

// ConsumerRecord is a record as returned to consumers.
type ConsumerRecord struct {
	Topic       string            `json:"topic"`
	Partition   int32             `json:"partition"`
	Offset      int64             `json:"offset"`
	Timestamp   int64             `json:"timestamp"`
	Key         *string           `json:"key,omitempty"`
	Value       *string           `json:"value"`
	Headers     map[string]string `json:"headers,omitempty"`
	LeaderEpoch int32             `json:"leader_epoch"`
	ProducerID  int64             `json:"producer_id"`
	Sequence    int64             `json:"sequence"` // -1 if not from an idempotent producer
}

// FetchResponse is the result of a consumer fetch.
type FetchResponse struct {
	Topic          string           `json:"topic"`
	Partition      int32            `json:"partition"`
	Records        []ConsumerRecord `json:"records"`
	HighWatermark  int64            `json:"high_watermark"`
	LogStartOffset int64            `json:"log_start_offset"`
	LogEndOffset   int64            `json:"log_end_offset"`
	NextOffset     int64            `json:"next_offset"`
}

// OffsetsResponse reports a partition's offsets.
type OffsetsResponse struct {
	Topic              string `json:"topic"`
	Partition          int32  `json:"partition"`
	LogStartOffset     int64  `json:"log_start_offset"`
	LogEndOffset       int64  `json:"log_end_offset"`
	HighWatermark      int64  `json:"high_watermark"`
	OffsetForTimestamp int64  `json:"offset_for_timestamp,omitempty"`
}

// GatewayProduceRequest is the topic-level (key-routed) produce request.
type GatewayProduceRequest struct {
	Acks    string   `json:"acks,omitempty"`
	Records []Record `json:"records"`
}

// GatewayRecordResult is the per-record outcome at the gateway.
type GatewayRecordResult struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Error     string `json:"error,omitempty"`
}

// GatewayProduceResponse lists results in request order.
type GatewayProduceResponse struct {
	Results []GatewayRecordResult `json:"results"`
}

// --- controller ------------------------------------------------------------

// BrokerHeartbeatRequest registers or refreshes a broker.
type BrokerHeartbeatRequest struct {
	BrokerID          int32            `json:"broker_id"`
	URL               string           `json:"url"`
	Incarnation       string           `json:"incarnation"`
	OfflinePartitions []TopicPartition `json:"offline_partitions,omitempty"`
}

// BrokerHeartbeatResponse grants a lease.
type BrokerHeartbeatResponse struct {
	LeaseMs         int64 `json:"lease_ms"`
	MetadataVersion int64 `json:"metadata_version"`
}

// AlterISRRequest is a leader asking the controller to change the ISR.
type AlterISRRequest struct {
	Topic       string  `json:"topic"`
	Partition   int32   `json:"partition"`
	LeaderID    int32   `json:"leader_id"`
	LeaderEpoch int32   `json:"leader_epoch"`
	ISR         []int32 `json:"isr"`
}

// CreateTopicRequest creates a topic. Config fields left zero take defaults.
type CreateTopicRequest struct {
	Name              string          `json:"name"`
	Partitions        int             `json:"partitions"`
	ReplicationFactor int             `json:"replication_factor"`
	Config            json.RawMessage `json:"config,omitempty"`
}

// AddPartitionsRequest grows a topic.
type AddPartitionsRequest struct {
	TotalPartitions int `json:"total_partitions"`
}

// ProducerIDResponse returns a newly allocated producer ID.
type ProducerIDResponse struct {
	ProducerID int64 `json:"producer_id"`
}

// --- consumer groups -------------------------------------------------------

// JoinGroupRequest joins (or rejoins) a group.
type JoinGroupRequest struct {
	MemberID           string   `json:"member_id,omitempty"`
	ClientID           string   `json:"client_id,omitempty"`
	Topics             []string `json:"topics"`
	SessionTimeoutMs   int64    `json:"session_timeout_ms,omitempty"`
	RebalanceTimeoutMs int64    `json:"rebalance_timeout_ms,omitempty"`
	Assignor           string   `json:"assignor,omitempty"`
}

// JoinGroupResponse carries the member's assignment for a generation.
type JoinGroupResponse struct {
	MemberID   string           `json:"member_id"`
	Generation int32            `json:"generation"`
	Assignment []TopicPartition `json:"assignment"`
	Members    int              `json:"members"`
}

// GroupHeartbeatRequest keeps a member alive.
type GroupHeartbeatRequest struct {
	MemberID   string `json:"member_id"`
	Generation int32  `json:"generation"`
}

// GroupHeartbeatResponse reports the group state.
type GroupHeartbeatResponse struct {
	Generation int32  `json:"generation"`
	State      string `json:"state"`
}

// LeaveGroupRequest removes a member.
type LeaveGroupRequest struct {
	MemberID string `json:"member_id"`
}

// OffsetCommit is a committed position: the offset of the next record the
// group should process (last processed + 1).
type OffsetCommit struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Metadata  string `json:"metadata,omitempty"`
	CommitMs  int64  `json:"commit_ms,omitempty"`
}

// CommitOffsetsRequest commits offsets for a member and generation.
// Generation -1 with an empty member ID is an administrative commit,
// allowed only when the group has no active members.
type CommitOffsetsRequest struct {
	MemberID   string         `json:"member_id,omitempty"`
	Generation int32          `json:"generation"`
	Offsets    []OffsetCommit `json:"offsets"`
}

// GroupOffsetsResponse lists committed offsets.
type GroupOffsetsResponse struct {
	Group   string         `json:"group"`
	Offsets []OffsetCommit `json:"offsets"`
}

// GroupMember describes a member.
type GroupMember struct {
	MemberID   string           `json:"member_id"`
	ClientID   string           `json:"client_id"`
	Topics     []string         `json:"topics"`
	Assignment []TopicPartition `json:"assignment"`
}

// GroupDescription describes a group.
type GroupDescription struct {
	Group           string        `json:"group"`
	State           string        `json:"state"`
	Generation      int32         `json:"generation"`
	Assignor        string        `json:"assignor"`
	Members         []GroupMember `json:"members"`
	Rebalances      int64         `json:"rebalances"`
	LastRebalanceMs int64         `json:"last_rebalance_ms"`
}

// PartitionLag is lag for one partition.
type PartitionLag struct {
	Topic         string `json:"topic"`
	Partition     int32  `json:"partition"`
	Committed     int64  `json:"committed_offset"`
	HighWatermark int64  `json:"high_watermark"`
	Lag           int64  `json:"lag"`
	Owner         string `json:"owner,omitempty"`
	Error         string `json:"error,omitempty"`
}

// GroupLagResponse is lag for a group.
type GroupLagResponse struct {
	Group      string         `json:"group"`
	TotalLag   int64          `json:"total_lag"`
	Partitions []PartitionLag `json:"partitions"`
}

// ResetOffsetsRequest moves a group's committed offsets. To is one of
// earliest, latest, offset, timestamp. Timestamp accepts RFC 3339 or unix ms.
type ResetOffsetsRequest struct {
	Topic     string `json:"topic"`
	To        string `json:"to"`
	Offset    int64  `json:"offset,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
	DryRun    bool   `json:"dry_run,omitempty"`
}
