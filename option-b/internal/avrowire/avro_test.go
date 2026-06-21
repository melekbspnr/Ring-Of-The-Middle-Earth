package avrowire

import (
	"encoding/json"
	"testing"

	"github.com/linkedin/goavro/v2"
)

func TestOrderSubmittedAvroRoundTrip(t *testing.T) {
	input := rawOrderBody{
		PlayerID:  "player-light",
		UnitID:    "ring-bearer",
		OrderType: "ASSIGN_ROUTE",
		Turn:      2,
		PathIDs:   []string{"shire-to-bree", "bree-to-weathertop"},
		Timestamp: 1710000000000,
	}
	rawJSON, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}

	subject, native, err := buildAvroNative("game.orders.raw", rawJSON)
	if err != nil {
		t.Fatalf("buildAvroNative: %v", err)
	}
	if subject != "game.orders.raw-value" {
		t.Fatalf("unexpected subject %q", subject)
	}

	codec, err := goavro.NewCodec(avroSchemaBySubject[subject])
	if err != nil {
		t.Fatalf("new codec: %v", err)
	}
	binaryPayload, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		t.Fatalf("binary from native: %v", err)
	}
	decodedNative, _, err := codec.NativeFromBinary(binaryPayload)
	if err != nil {
		t.Fatalf("native from binary: %v", err)
	}

	roundTripJSON, err := avroNativeToJSON("game.orders.raw", subject, decodedNative)
	if err != nil {
		t.Fatalf("avroNativeToJSON: %v", err)
	}

	var out rawOrderBody
	if err := json.Unmarshal(roundTripJSON, &out); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}

	if out.PlayerID != input.PlayerID || out.UnitID != input.UnitID || out.OrderType != input.OrderType {
		t.Fatalf("envelope mismatch: %#v", out)
	}
	if out.Turn != input.Turn || out.Timestamp != input.Timestamp {
		t.Fatalf("turn/timestamp mismatch: %#v", out)
	}
	if len(out.PathIDs) != len(input.PathIDs) || out.PathIDs[1] != input.PathIDs[1] {
		t.Fatalf("pathIds mismatch: %#v", out.PathIDs)
	}
}

func TestOrderValidatedV2RemainsReadableByV1Consumer(t *testing.T) {
	raw := rawOrderBody{
		PlayerID:  "player-light",
		UnitID:    "ring-bearer",
		OrderType: "ASSIGN_ROUTE",
		Turn:      3,
		PathIDs:   []string{"shire-to-bree"},
		Timestamp: 1710000001234,
	}
	rawJSON, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal raw: %v", err)
	}

	score := 7
	validated := validatedOrderBody{
		PlayerID:        raw.PlayerID,
		UnitID:          raw.UnitID,
		OrderType:       raw.OrderType,
		Payload:         rawJSON,
		Turn:            raw.Turn,
		Timestamp:       raw.Timestamp,
		RouteRiskScore:  &score,
		ThreatenedPaths: []string{"shire-to-bree"},
		BlockedPaths:    []string{},
	}
	validatedJSON, err := json.Marshal(validated)
	if err != nil {
		t.Fatalf("marshal validated: %v", err)
	}

	subject, native, err := buildAvroNative("game.orders.validated", validatedJSON)
	if err != nil {
		t.Fatalf("buildAvroNative: %v", err)
	}
	codec, err := goavro.NewCodec(avroSchemaBySubject[subject])
	if err != nil {
		t.Fatalf("new codec: %v", err)
	}
	binaryPayload, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		t.Fatalf("binary from native: %v", err)
	}
	decodedNative, _, err := codec.NativeFromBinary(binaryPayload)
	if err != nil {
		t.Fatalf("native from binary: %v", err)
	}

	consumerJSON, err := avroNativeToJSON("game.orders.validated", subject, decodedNative)
	if err != nil {
		t.Fatalf("avroNativeToJSON: %v", err)
	}

	var legacy struct {
		PlayerID  string `json:"playerId"`
		UnitID    string `json:"unitId"`
		OrderType string `json:"orderType"`
		Payload   []byte `json:"payload"`
		Turn      int    `json:"turn"`
		Timestamp int64  `json:"timestamp"`
	}
	if err := json.Unmarshal(consumerJSON, &legacy); err != nil {
		t.Fatalf("legacy consumer unmarshal failed: %v", err)
	}
	if legacy.PlayerID != raw.PlayerID || legacy.UnitID != raw.UnitID || legacy.OrderType != raw.OrderType {
		t.Fatalf("legacy envelope mismatch: %#v", legacy)
	}

	var legacyRaw rawOrderBody
	if err := json.Unmarshal(legacy.Payload, &legacyRaw); err != nil {
		t.Fatalf("legacy payload unmarshal failed: %v", err)
	}
	if legacyRaw.PathIDs[0] != "shire-to-bree" {
		t.Fatalf("legacy payload lost route data: %#v", legacyRaw)
	}
}
