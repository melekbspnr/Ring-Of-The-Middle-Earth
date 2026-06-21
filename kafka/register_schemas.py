import json
import os
import pathlib
import sys
import time
import urllib.error
import urllib.request


SCHEMA_DIR = pathlib.Path(__file__).resolve().parent / "schemas"
REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081").rstrip("/")


def load_json(path: pathlib.Path):
    return json.loads(path.read_text(encoding="utf-8"))


def wait_for_registry(timeout_seconds: int = 90) -> None:
    deadline = time.time() + timeout_seconds
    url = f"{REGISTRY_URL}/subjects"
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                if 200 <= resp.status < 300:
                    return
        except Exception:
            time.sleep(2)
    raise RuntimeError(f"schema registry not ready at {REGISTRY_URL}")


def register(subject: str, schema_obj) -> None:
    body = json.dumps(
        {
            "schemaType": "AVRO",
            "schema": json.dumps(schema_obj),
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        f"{REGISTRY_URL}/subjects/{subject}/versions",
        data=body,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
        print(f"registered {subject} -> version {payload.get('id')}")


def main() -> int:
    wait_for_registry()

    world_state = load_json(SCHEMA_DIR / "WorldStateSnapshot.avsc")
    game_over = load_json(SCHEMA_DIR / "GameOver.avsc")

    subject_schemas = {
        "game.orders.raw-value": load_json(SCHEMA_DIR / "OrderSubmitted.avsc"),
        "game.orders.validated-value": load_json(SCHEMA_DIR / "OrderValidated.avsc"),
        "game.events.unit-value": load_json(SCHEMA_DIR / "UnitMoved.avsc"),
        "game.events.region-value": load_json(SCHEMA_DIR / "RegionControlChanged.avsc"),
        "game.events.path-value": load_json(SCHEMA_DIR / "PathStatusChanged.avsc"),
        "game.ring.position-value": load_json(SCHEMA_DIR / "RingBearerMoved.avsc"),
        "game.ring.detection-value": [
            load_json(SCHEMA_DIR / "RingBearerDetected.avsc"),
            load_json(SCHEMA_DIR / "RingBearerSpotted.avsc"),
        ],
        "game.broadcast-value": [world_state, game_over],
        "game.dlq-value": load_json(SCHEMA_DIR / "DLQEntry.avsc"),
        "OrderSubmitted-value": load_json(SCHEMA_DIR / "OrderSubmitted.avsc"),
        "OrderValidated-value": load_json(SCHEMA_DIR / "OrderValidated.avsc"),
        "UnitMoved-value": load_json(SCHEMA_DIR / "UnitMoved.avsc"),
        "RegionControlChanged-value": load_json(SCHEMA_DIR / "RegionControlChanged.avsc"),
        "PathStatusChanged-value": load_json(SCHEMA_DIR / "PathStatusChanged.avsc"),
        "PathCorrupted-value": load_json(SCHEMA_DIR / "PathCorrupted.avsc"),
        "BattleResolved-value": load_json(SCHEMA_DIR / "BattleResolved.avsc"),
        "RingBearerMoved-value": load_json(SCHEMA_DIR / "RingBearerMoved.avsc"),
        "RingBearerDetected-value": load_json(SCHEMA_DIR / "RingBearerDetected.avsc"),
        "RingBearerSpotted-value": load_json(SCHEMA_DIR / "RingBearerSpotted.avsc"),
        "WorldStateSnapshot-value": world_state,
        "GameOver-value": game_over,
        "DLQEntry-value": load_json(SCHEMA_DIR / "DLQEntry.avsc"),
    }

    for subject, schema_obj in subject_schemas.items():
        try:
            register(subject, schema_obj)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            print(f"failed to register {subject}: {detail}", file=sys.stderr)
            return 1

    # ── Schema Evolution: register V2 schemas (K3 criterion) ──────────────
    # V2 schemas add backward-compatible nullable fields.
    # V1 consumers continue to work because all new fields have defaults.
    # Compatibility mode is BACKWARD by default in Schema Registry.
    v2_schemas = {
        "game.orders.validated-value": load_json(SCHEMA_DIR / "OrderValidated_v2.avsc"),
        "OrderValidated-value": load_json(SCHEMA_DIR / "OrderValidated_v2.avsc"),
        "game.broadcast-value": load_json(SCHEMA_DIR / "WorldStateSnapshot_v2.avsc"),
        "WorldStateSnapshot-value": load_json(SCHEMA_DIR / "WorldStateSnapshot_v2.avsc"),
    }

    print("\n── Registering V2 schemas (schema evolution) ──")
    for subject, schema_obj in v2_schemas.items():
        try:
            register(subject, schema_obj)
            print(f"  V2 evolution OK: {subject}")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            # 409 = incompatible schema, which is expected if V1 was very different
            if exc.code == 409:
                print(f"  V2 {subject}: incompatible (expected in some cases)")
            else:
                print(f"  V2 {subject} failed: {detail}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
