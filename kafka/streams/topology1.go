// Package streams documents Kafka Streams Topology 1: Order Validation.
// Section 11 of the spec.
//
// Source: game.orders.raw
// Sinks:  game.orders.validated (valid orders)
//
//	game.dlq              (invalid orders with error codes)
//
// KTables:
//
//	TurnKTable  - current turn, from game.session
//	UnitKTable  - unit states, from game.events.unit
//	PathKTable  - path states, from game.events.path
//
// 8 Validation Rules:
//  1. order.turn does not match current turn           -> WRONG_TURN
//  2. Unit not owned by submitting player's side       -> NOT_YOUR_UNIT
//  3. Ring Bearer route: next path is BLOCKED          -> PATH_BLOCKED
//  4. Ring Bearer route: path not in assigned route    -> INVALID_PATH
//  5. BlockPath/SearchPath: unit not at endpoint       -> UNIT_NOT_ADJACENT
//  6. AttackRegion: target not adjacent or wrong ctrl  -> INVALID_TARGET
//  7. MaiaAbility: unit cooldown > 0                   -> ABILITY_ON_COOLDOWN
//  8. Same unitId appears more than once this turn     -> DUPLICATE_UNIT_ORDER
//
// In this repository the topology is mirrored by the Go validation stage in
// internal/kafkaclient/order_ingest.go rather than a separate JVM process.
// This file remains the topology contract and validation-rule inventory.
package streams

// OrderValidationTopology describes the Topology 1 configuration.
// For Option B, equivalent validation is performed before the engine buffers
// orders, and invalid messages are diverted to game.dlq.

// ValidationRule names all 8 error codes for reference.
var ValidationRules = []struct {
	Rule      int
	Condition string
	ErrorCode string
}{
	{1, "order.turn does not match current turn in TurnKTable", "WRONG_TURN"},
	{2, "unit.side != playerSide from game.session", "NOT_YOUR_UNIT"},
	{3, "ASSIGN_ROUTE/REDIRECT_UNIT: next path status == BLOCKED in PathKTable", "PATH_BLOCKED"},
	{4, "ASSIGN_ROUTE: pathId not in unit.assignedRoute in UnitKTable", "INVALID_PATH"},
	{5, "BLOCK_PATH/SEARCH_PATH: unit.currentRegion not in path endpoints", "UNIT_NOT_ADJACENT"},
	{6, "ATTACK_REGION: target not adjacent or not enemy-controlled", "INVALID_TARGET"},
	{7, "MAIA_ABILITY: unit.cooldown > 0 in UnitKTable", "ABILITY_ON_COOLDOWN"},
	{8, "same unitId appears >1 time in this turn's order batch", "DUPLICATE_UNIT_ORDER"},
}
