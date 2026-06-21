package avrowire

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"ring-of-the-middle-earth/internal/game"

	"github.com/linkedin/goavro/v2"
)

const avroMagicByte byte = 0

type avroCodecRef struct {
	subject string
	id      int
	codec   *goavro.Codec
}

type Registry struct {
	url       string
	client    *http.Client
	mu        sync.RWMutex
	bySubject map[string]*avroCodecRef
	byID      map[int]*avroCodecRef
}

type rawOrderBody struct {
	OrderType    string   `json:"orderType"`
	PlayerID     string   `json:"playerId"`
	UnitID       string   `json:"unitId"`
	Turn         int      `json:"turn"`
	PathIDs      []string `json:"pathIds"`
	NewPathIDs   []string `json:"newPathIds"`
	TargetPathID string   `json:"targetPathId"`
	TargetRegion string   `json:"targetRegion"`
	Timestamp    int64    `json:"timestamp"`
}

type validatedOrderBody struct {
	PlayerID        string   `json:"playerId"`
	UnitID          string   `json:"unitId"`
	OrderType       string   `json:"orderType"`
	Payload         []byte   `json:"payload"`
	Turn            int      `json:"turn"`
	Timestamp       int64    `json:"timestamp"`
	RouteRiskScore  *int     `json:"routeRiskScore"`
	ThreatenedPaths []string `json:"threatenedPaths"`
	BlockedPaths    []string `json:"blockedPaths"`
}

func NewRegistry() *Registry {
	url := strings.TrimRight(strings.TrimSpace(os.Getenv("SCHEMA_REGISTRY_URL")), "/")
	if url == "" {
		url = "http://localhost:8081"
	}
	return &Registry{
		url:       url,
		client:    &http.Client{Timeout: 10 * time.Second},
		byID:      map[int]*avroCodecRef{},
		bySubject: map[string]*avroCodecRef{},
	}
}

func (r *Registry) Encode(topic string, payload []byte) ([]byte, error) {
	subject, native, err := buildAvroNative(topic, payload)
	if err != nil || subject == "" {
		return payload, err
	}
	ref, err := r.ensureSubject(subject)
	if err != nil {
		return nil, err
	}
	binaryPayload, err := ref.codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, err
	}
	out := bytes.NewBuffer(make([]byte, 0, 5+len(binaryPayload)))
	out.WriteByte(avroMagicByte)
	_ = binary.Write(out, binary.BigEndian, uint32(ref.id))
	out.Write(binaryPayload)
	return out.Bytes(), nil
}

func (r *Registry) Decode(topic string, payload []byte) ([]byte, error) {
	if len(payload) < 5 || payload[0] != avroMagicByte {
		return payload, nil
	}
	if err := r.ensureTopicSubjects(topic); err != nil {
		return nil, err
	}
	schemaID := int(binary.BigEndian.Uint32(payload[1:5]))

	r.mu.RLock()
	ref := r.byID[schemaID]
	r.mu.RUnlock()
	if ref == nil {
		return nil, fmt.Errorf("unknown schema id %d for topic %s", schemaID, topic)
	}

	native, _, err := ref.codec.NativeFromBinary(payload[5:])
	if err != nil {
		return nil, err
	}
	return avroNativeToJSON(topic, ref.subject, native)
}

func (r *Registry) ensureTopicSubjects(topic string) error {
	for _, subject := range avroSubjectsForTopic(topic) {
		if _, err := r.ensureSubject(subject); err != nil {
			return err
		}
	}
	return nil
}

func (r *Registry) ensureSubject(subject string) (*avroCodecRef, error) {
	r.mu.RLock()
	if ref := r.bySubject[subject]; ref != nil {
		r.mu.RUnlock()
		return ref, nil
	}
	r.mu.RUnlock()

	schemaText, ok := avroSchemaBySubject[subject]
	if !ok {
		return nil, fmt.Errorf("no local Avro schema for subject %s", subject)
	}
	codec, err := goavro.NewCodec(schemaText)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/subjects/%s/versions/latest", r.url, subject), nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("schema registry latest %s: %s", subject, strings.TrimSpace(string(body)))
	}

	var latest struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&latest); err != nil {
		return nil, err
	}

	ref := &avroCodecRef{subject: subject, id: latest.ID, codec: codec}
	r.mu.Lock()
	r.bySubject[subject] = ref
	r.byID[latest.ID] = ref
	r.mu.Unlock()
	return ref, nil
}

func buildAvroNative(topic string, payload []byte) (string, map[string]interface{}, error) {
	switch topic {
	case "game.orders.raw":
		var body rawOrderBody
		if err := json.Unmarshal(payload, &body); err != nil {
			return "", nil, err
		}
		orderPayload, err := json.Marshal(orderSpecificPayload(body))
		if err != nil {
			return "", nil, err
		}
		return "game.orders.raw-value", map[string]interface{}{
			"playerId":  body.PlayerID,
			"unitId":    body.UnitID,
			"orderType": body.OrderType,
			"payload":   orderPayload,
			"turn":      body.Turn,
			"timestamp": body.Timestamp,
		}, nil

	case "game.orders.validated":
		var body validatedOrderBody
		if err := json.Unmarshal(payload, &body); err != nil {
			return "", nil, err
		}
		var raw rawOrderBody
		if len(body.Payload) > 0 {
			if err := json.Unmarshal(body.Payload, &raw); err != nil {
				return "", nil, err
			}
		}
		orderPayload, err := json.Marshal(orderSpecificPayload(raw))
		if err != nil {
			return "", nil, err
		}
		risk := goavro.Union("null", nil)
		if body.RouteRiskScore != nil {
			risk = goavro.Union("int", *body.RouteRiskScore)
		}
		return "game.orders.validated-value", map[string]interface{}{
			"playerId":        body.PlayerID,
			"unitId":          body.UnitID,
			"orderType":       body.OrderType,
			"payload":         orderPayload,
			"turn":            body.Turn,
			"timestamp":       body.Timestamp,
			"routeRiskScore":  risk,
			"threatenedPaths": toInterfaceSlice(body.ThreatenedPaths),
			"blockedPaths":    toInterfaceSlice(body.BlockedPaths),
		}, nil

	case "game.events.unit":
		var ev struct {
			UnitID    string  `json:"unitId"`
			From      string  `json:"from"`
			To        string  `json:"to"`
			Strength  *int    `json:"strength,omitempty"`
			Status    *string `json:"status,omitempty"`
			Cooldown  *int    `json:"cooldown,omitempty"`
			Turn      int     `json:"turn"`
			Timestamp int64   `json:"timestamp"`
		}
		if err := json.Unmarshal(payload, &ev); err != nil {
			return "", nil, err
		}
		strength := goavro.Union("null", nil)
		if ev.Strength != nil {
			strength = goavro.Union("int", *ev.Strength)
		}
		status := goavro.Union("null", nil)
		if ev.Status != nil {
			status = goavro.Union("string", *ev.Status)
		}
		cooldown := goavro.Union("null", nil)
		if ev.Cooldown != nil {
			cooldown = goavro.Union("int", *ev.Cooldown)
		}
		return "game.events.unit-value", map[string]interface{}{
			"unitId":    ev.UnitID,
			"from":      ev.From,
			"to":        ev.To,
			"strength":  strength,
			"status":    status,
			"cooldown":  cooldown,
			"turn":      ev.Turn,
			"timestamp": ev.Timestamp,
		}, nil

	case "game.events.region":
		var ev map[string]interface{}
		if err := json.Unmarshal(payload, &ev); err != nil {
			return "", nil, err
		}
		return "game.events.region-value", map[string]interface{}{
			"regionId":      stringValue(ev["regionId"]),
			"newController": stringValue(ev["newController"]),
			"turn":          intValue(ev["turn"]),
			"timestamp":     int64Value(ev["timestamp"]),
		}, nil

	case "game.events.path":
		var ev map[string]interface{}
		if err := json.Unmarshal(payload, &ev); err != nil {
			return "", nil, err
		}
		return "game.events.path-value", map[string]interface{}{
			"pathId":            stringValue(ev["pathId"]),
			"newStatus":         stringValue(ev["newStatus"]),
			"surveillanceLevel": intValue(ev["surveillanceLevel"]),
			"tempOpenTurns":     intValue(ev["tempOpenTurns"]),
			"turn":              intValue(ev["turn"]),
			"timestamp":         int64Value(ev["timestamp"]),
		}, nil

	case "game.ring.position":
		var ev map[string]interface{}
		if err := json.Unmarshal(payload, &ev); err != nil {
			return "", nil, err
		}
		return "game.ring.position-value", map[string]interface{}{
			"trueRegion": stringValue(ev["trueRegion"]),
			"turn":       intValue(ev["turn"]),
			"timestamp":  int64Value(ev["timestamp"]),
		}, nil

	case "game.ring.detection":
		var ev map[string]interface{}
		if err := json.Unmarshal(payload, &ev); err != nil {
			return "", nil, err
		}
		if _, ok := ev["pathId"]; ok {
			return "RingBearerSpotted-value", map[string]interface{}{
				"pathId":    stringValue(ev["pathId"]),
				"turn":      intValue(ev["turn"]),
				"timestamp": int64Value(ev["timestamp"]),
			}, nil
		}
		return "RingBearerDetected-value", map[string]interface{}{
			"regionId":  stringValue(ev["regionId"]),
			"turn":      intValue(ev["turn"]),
			"timestamp": int64Value(ev["timestamp"]),
		}, nil

	case "game.broadcast":
		var obj map[string]interface{}
		if err := json.Unmarshal(payload, &obj); err != nil {
			return "", nil, err
		}
		if _, ok := obj["winner"]; ok {
			return "GameOver-value", map[string]interface{}{
				"winner":    stringValue(obj["winner"]),
				"cause":     stringValue(obj["cause"]),
				"turn":      intValue(obj["turn"]),
				"timestamp": int64Value(obj["timestamp"]),
			}, nil
		}

		var snap game.WorldStateSnapshot
		if err := json.Unmarshal(payload, &snap); err != nil {
			return "", nil, err
		}
		units := make([]interface{}, 0, len(snap.Units))
		for _, u := range snap.Units {
			units = append(units, map[string]interface{}{
				"id":            u.ID,
				"currentRegion": u.CurrentRegion,
				"strength":      u.Strength,
				"status":        u.Status,
				"side":          u.Side,
			})
		}
		regions := make([]interface{}, 0, len(snap.Regions))
		for _, r := range snap.Regions {
			regions = append(regions, map[string]interface{}{
				"id":           r.ID,
				"controlledBy": r.ControlledBy,
				"threatLevel":  r.ThreatLevel,
				"fortified":    r.Fortified,
			})
		}
		paths := make([]interface{}, 0, len(snap.Paths))
		for _, p := range snap.Paths {
			paths = append(paths, map[string]interface{}{
				"id":                p.ID,
				"newStatus":         p.NewStatus,
				"surveillanceLevel": p.SurveillanceLevel,
				"tempOpenTurns":     p.TempOpenTurns,
			})
		}
		trueRegion := goavro.Union("null", nil)
		if snap.RingBearerTrueRegion != "" {
			trueRegion = goavro.Union("string", snap.RingBearerTrueRegion)
		}
		return "WorldStateSnapshot-value", map[string]interface{}{
			"turn":                 snap.Turn,
			"units":                units,
			"regions":              regions,
			"paths":                paths,
			"ringBearerTrueRegion": trueRegion,
			"timestamp":            snap.Timestamp,
		}, nil

	case "game.dlq":
		var dlq map[string]interface{}
		if err := json.Unmarshal(payload, &dlq); err != nil {
			return "", nil, err
		}
		return "game.dlq-value", map[string]interface{}{
			"originalTopic": stringValue(dlq["originalTopic"]),
			"partition":     intValue(dlq["partition"]),
			"offset":        int64Value(dlq["offset"]),
			"errorCode":     stringValue(dlq["errorCode"]),
			"errorMessage":  stringValue(dlq["errorMessage"]),
			"rawPayload":    []byte(stringValue(dlq["rawPayload"])),
			"timestamp":     int64Value(dlq["timestamp"]),
		}, nil
	}

	return "", nil, nil
}

func avroNativeToJSON(topic, subject string, native interface{}) ([]byte, error) {
	data := normalizeAvroValue(native)

	switch topic {
	case "game.orders.raw":
		record := data.(map[string]interface{})
		payload, err := buildRawOrderJSON(record)
		if err != nil {
			return nil, err
		}
		return payload, nil

	case "game.orders.validated":
		record := data.(map[string]interface{})
		payload, err := buildValidatedOrderJSON(record)
		if err != nil {
			return nil, err
		}
		return payload, nil

	case "game.dlq":
		record := data.(map[string]interface{})
		record["rawPayload"] = string(bytesValue(record["rawPayload"]))
		return json.Marshal(record)
	}

	return json.Marshal(data)
}

func buildRawOrderJSON(record map[string]interface{}) ([]byte, error) {
	raw := rawOrderBody{
		PlayerID:  stringValue(record["playerId"]),
		UnitID:    stringValue(record["unitId"]),
		OrderType: stringValue(record["orderType"]),
		Turn:      intValue(record["turn"]),
		Timestamp: int64Value(record["timestamp"]),
	}
	applyOrderSpecificPayload(&raw, bytesValue(record["payload"]))
	return json.Marshal(raw)
}

func buildValidatedOrderJSON(record map[string]interface{}) ([]byte, error) {
	raw := rawOrderBody{
		PlayerID:  stringValue(record["playerId"]),
		UnitID:    stringValue(record["unitId"]),
		OrderType: stringValue(record["orderType"]),
		Turn:      intValue(record["turn"]),
		Timestamp: int64Value(record["timestamp"]),
	}
	applyOrderSpecificPayload(&raw, bytesValue(record["payload"]))
	rawPayload, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}

	validated := validatedOrderBody{
		PlayerID:        raw.PlayerID,
		UnitID:          raw.UnitID,
		OrderType:       raw.OrderType,
		Payload:         rawPayload,
		Turn:            raw.Turn,
		Timestamp:       raw.Timestamp,
		ThreatenedPaths: stringSlice(record["threatenedPaths"]),
		BlockedPaths:    stringSlice(record["blockedPaths"]),
	}
	if v := record["routeRiskScore"]; v != nil {
		score := intValue(v)
		validated.RouteRiskScore = &score
	}
	return json.Marshal(validated)
}

func orderSpecificPayload(body rawOrderBody) map[string]interface{} {
	out := map[string]interface{}{}
	if len(body.PathIDs) > 0 {
		out["pathIds"] = body.PathIDs
	}
	if len(body.NewPathIDs) > 0 {
		out["newPathIds"] = body.NewPathIDs
	}
	if body.TargetPathID != "" {
		out["targetPathId"] = body.TargetPathID
	}
	if body.TargetRegion != "" {
		out["targetRegion"] = body.TargetRegion
	}
	return out
}

func applyOrderSpecificPayload(body *rawOrderBody, payload []byte) {
	if len(payload) == 0 {
		return
	}
	var extra struct {
		PathIDs      []string `json:"pathIds"`
		NewPathIDs   []string `json:"newPathIds"`
		TargetPathID string   `json:"targetPathId"`
		TargetRegion string   `json:"targetRegion"`
	}
	if err := json.Unmarshal(payload, &extra); err != nil {
		return
	}
	body.PathIDs = append(body.PathIDs, extra.PathIDs...)
	body.NewPathIDs = append(body.NewPathIDs, extra.NewPathIDs...)
	body.TargetPathID = extra.TargetPathID
	body.TargetRegion = extra.TargetRegion
}

func avroSubjectsForTopic(topic string) []string {
	switch topic {
	case "game.orders.raw":
		return []string{"game.orders.raw-value"}
	case "game.orders.validated":
		return []string{"game.orders.validated-value"}
	case "game.events.unit":
		return []string{"game.events.unit-value"}
	case "game.events.region":
		return []string{"game.events.region-value"}
	case "game.events.path":
		return []string{"game.events.path-value"}
	case "game.ring.position":
		return []string{"game.ring.position-value"}
	case "game.ring.detection":
		return []string{"RingBearerDetected-value", "RingBearerSpotted-value"}
	case "game.broadcast":
		return []string{"WorldStateSnapshot-value", "GameOver-value"}
	case "game.dlq":
		return []string{"game.dlq-value"}
	default:
		return nil
	}
}

func toInterfaceSlice(values []string) []interface{} {
	out := make([]interface{}, 0, len(values))
	for _, v := range values {
		out = append(out, v)
	}
	return out
}

func normalizeAvroValue(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		if len(t) == 1 {
			for k, inner := range t {
				if isUnionMarker(k) {
					return normalizeAvroValue(inner)
				}
			}
		}
		out := make(map[string]interface{}, len(t))
		for k, inner := range t {
			out[k] = normalizeAvroValue(inner)
		}
		return out
	case []interface{}:
		out := make([]interface{}, 0, len(t))
		for _, inner := range t {
			out = append(out, normalizeAvroValue(inner))
		}
		return out
	default:
		return v
	}
}

func isUnionMarker(key string) bool {
	switch key {
	case "null", "string", "bytes", "int", "long", "boolean":
		return true
	default:
		return false
	}
}

func stringSlice(v interface{}) []string {
	items, ok := v.([]interface{})
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, stringValue(item))
	}
	return out
}

func stringValue(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return fmt.Sprint(t)
	}
}

func intValue(v interface{}) int {
	switch t := v.(type) {
	case int:
		return t
	case int32:
		return int(t)
	case int64:
		return int(t)
	case float64:
		return int(t)
	default:
		return 0
	}
}

func int64Value(v interface{}) int64 {
	switch t := v.(type) {
	case int:
		return int64(t)
	case int32:
		return int64(t)
	case int64:
		return t
	case float64:
		return int64(t)
	default:
		return 0
	}
}

func bytesValue(v interface{}) []byte {
	switch t := v.(type) {
	case []byte:
		return t
	case string:
		return []byte(t)
	default:
		return nil
	}
}

var avroSchemaBySubject = map[string]string{
	"game.orders.raw-value":       `{"type":"record","name":"OrderSubmitted","namespace":"rotr.orders","fields":[{"name":"playerId","type":"string"},{"name":"unitId","type":"string"},{"name":"orderType","type":"string"},{"name":"payload","type":"bytes"},{"name":"turn","type":"int"},{"name":"timestamp","type":"long"}]}`,
	"game.orders.validated-value": `{"type":"record","name":"OrderValidated","namespace":"rotr.orders","fields":[{"name":"playerId","type":"string"},{"name":"unitId","type":"string"},{"name":"orderType","type":"string"},{"name":"payload","type":"bytes"},{"name":"turn","type":"int"},{"name":"timestamp","type":"long"},{"name":"routeRiskScore","type":["null","int"],"default":null},{"name":"threatenedPaths","type":{"type":"array","items":"string"},"default":[]},{"name":"blockedPaths","type":{"type":"array","items":"string"},"default":[]}]}`,
	"game.events.unit-value":      `{"type":"record","name":"UnitMoved","namespace":"rotr.events","fields":[{"name":"unitId","type":"string"},{"name":"from","type":"string"},{"name":"to","type":"string"},{"name":"strength","type":["null","int"],"default":null},{"name":"status","type":["null","string"],"default":null},{"name":"cooldown","type":["null","int"],"default":null},{"name":"turn","type":"int"},{"name":"timestamp","type":"long"}]}`,
	"game.events.region-value":    `{"type":"record","name":"RegionControlChanged","namespace":"rotr.events","fields":[{"name":"regionId","type":"string"},{"name":"newController","type":"string"},{"name":"turn","type":"int"},{"name":"timestamp","type":"long"}]}`,
	"game.events.path-value":      `{"type":"record","name":"PathStatusChanged","namespace":"rotr.events","fields":[{"name":"pathId","type":"string"},{"name":"newStatus","type":"string"},{"name":"surveillanceLevel","type":"int"},{"name":"tempOpenTurns","type":"int"},{"name":"turn","type":"int"},{"name":"timestamp","type":"long"}]}`,
	"game.ring.position-value":    `{"type":"record","name":"RingBearerMoved","namespace":"rotr.events","fields":[{"name":"trueRegion","type":"string"},{"name":"turn","type":"int"},{"name":"timestamp","type":"long"}]}`,
	"RingBearerDetected-value":    `{"type":"record","name":"RingBearerDetected","namespace":"rotr.events","fields":[{"name":"regionId","type":"string"},{"name":"turn","type":"int"},{"name":"timestamp","type":"long"}]}`,
	"RingBearerSpotted-value":     `{"type":"record","name":"RingBearerSpotted","namespace":"rotr.events","fields":[{"name":"pathId","type":"string"},{"name":"turn","type":"int"},{"name":"timestamp","type":"long"}]}`,
	"WorldStateSnapshot-value":    `{"type":"record","name":"WorldStateSnapshot","namespace":"rotr.broadcast","fields":[{"name":"turn","type":"int"},{"name":"units","type":{"type":"array","items":{"type":"record","name":"UnitPublic","fields":[{"name":"id","type":"string"},{"name":"currentRegion","type":"string"},{"name":"strength","type":"int"},{"name":"status","type":"string"},{"name":"side","type":"string"}]}}},{"name":"regions","type":{"type":"array","items":{"type":"record","name":"RegionPublic","fields":[{"name":"id","type":"string"},{"name":"controlledBy","type":"string"},{"name":"threatLevel","type":"int"},{"name":"fortified","type":"boolean"}]}}},{"name":"paths","type":{"type":"array","items":{"type":"record","name":"PathPublic","fields":[{"name":"id","type":"string"},{"name":"newStatus","type":"string"},{"name":"surveillanceLevel","type":"int"},{"name":"tempOpenTurns","type":"int"}]}},"default":[]},{"name":"ringBearerTrueRegion","type":["null","string"],"default":null},{"name":"timestamp","type":"long"}]}`,
	"GameOver-value":              `{"type":"record","name":"GameOver","namespace":"rotr.broadcast","fields":[{"name":"winner","type":"string"},{"name":"cause","type":"string"},{"name":"turn","type":"int"},{"name":"timestamp","type":"long"}]}`,
	"game.dlq-value":              `{"type":"record","name":"DLQEntry","namespace":"rotr.dlq","fields":[{"name":"originalTopic","type":"string"},{"name":"partition","type":"int"},{"name":"offset","type":"long"},{"name":"errorCode","type":"string"},{"name":"errorMessage","type":"string"},{"name":"rawPayload","type":"bytes"},{"name":"timestamp","type":"long"}]}`,
}
