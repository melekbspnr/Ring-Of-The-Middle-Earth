param(
    [Parameter(Mandatory=$true)]
    [string]$PlayerId,

    [Parameter(Mandatory=$true)]
    [int]$Turn
)

Write-Host "======================================================" -ForegroundColor Cyan
Write-Host " SCENARIO 3 - FAULT TOLERANCE & EXACTLY-ONCE TEST" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan

# ── ADIM 1: Aktif lideri bul ──────────────────────────────────────────────────
Write-Host "`n[1] Aktif Lider sunucu sorgulanıyor..." -ForegroundColor Yellow
$stateResponse = Invoke-RestMethod -Uri "http://localhost/game/state?side=light&playerId=$PlayerId"
$leader = $stateResponse.leaderId
if (-not $leader) { $leader = "go-1" }
Write-Host "    Aktif Lider: $leader  (Turn: $Turn)" -ForegroundColor Magenta

# ── ADIM 2: DESTROY_RING emrini gonder ───────────────────────────────────────
Write-Host "[2] DESTROY_RING emri gonderiliyor (Turn=$Turn)..." -ForegroundColor Yellow
$body = @{
    orderType = "DESTROY_RING"
    playerId  = $PlayerId
    unitId    = "ring-bearer"
    turn      = $Turn
} | ConvertTo-Json -Depth 5

try {
    Invoke-RestMethod -Uri "http://localhost/order?playerId=$PlayerId" `
                      -Method Post `
                      -Headers @{ "X-Side" = "light" } `
                      -Body $body `
                      -ContentType "application/json" | Out-Null
    Write-Host "    Emir kabul edildi (202 Accepted)." -ForegroundColor Green
} catch {
    Write-Host "    HATA: Emir gonderilemedi: $_" -ForegroundColor Red
    exit 1
}

# ── ADIM 3: Turn'un kapanmasini bekle (8sn tur + 3sn buffer = 11sn) ──────────
# Bu sure zarfinda Lider, DESTROY_RING emrini 'game.orders.validated' topic'ine
# commit ETMIS olur. Boylece emir kalici hale gelir.
Write-Host "[3] Mevcut turun kapanmasi ve emrin Kafka'ya commit olmasi bekleniyor (11 sn)..." -ForegroundColor Yellow
Start-Sleep -Seconds 11
Write-Host "    Tur kapandi. Emir artik Kafka'da kalici." -ForegroundColor Green

# ── ADIM 4: Lider tekrar kontrol et (lider degismis olabilir) ────────────────
$stateResponse2 = Invoke-RestMethod -Uri "http://localhost/game/state?side=light&playerId=$PlayerId"
$leader2 = $stateResponse2.leaderId
if ($leader2) { $leader = $leader2 }
$currentTurn = $stateResponse2.turn
Write-Host "[4] Yeni tur baslamis. Su anki Lider: $leader  Turn: $currentTurn" -ForegroundColor Magenta

# ── ADIM 5: GameOver islemi baslasin, tam 15sn sleep'in icindeyken oldur ──────
# Sonraki tur ($Turn+1) islenirken lider GameOver hesaplayacak ve 15sn donacak.
# Biz o donmaya girmesi icin ~4sn bekliyoruz, sonra kill.
Write-Host "[5] GameOver islemi baslamasi bekleniyor (4 sn)..." -ForegroundColor Yellow
Start-Sleep -Seconds 4

Write-Host "[6] LIDER ($leader) TAM GAMEOVER TRANSACTION ORTASINDA COKERTILIYOR!" -ForegroundColor Red
docker kill $leader | Out-Null
Write-Host "    $leader olduruldu! Diger sunucular (follower'lar) devralacak." -ForegroundColor Green

# ── ADIM 6: Rebalance ve yeni lider secimi ───────────────────────────────────
Write-Host "[7] Kafka Consumer Rebalance + yeni lider secimi bekleniyor (20 sn)..." -ForegroundColor Yellow
Start-Sleep -Seconds 20

# ── ADIM 7: Eski lideri geri getir ───────────────────────────────────────────
Write-Host "[8] Eski lider ($leader) yeniden baslatiliyor (recovery)..." -ForegroundColor Yellow
docker start $leader | Out-Null

# ── ADIM 8: Kafka'yi dogrula ─────────────────────────────────────────────────
Write-Host "[9] Kafka 'game.broadcast' topic dogrulaniyor (GameOver sayisi)..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

$kafkaLogs = docker exec kafka-1 /bin/sh -c `
    "kafka-console-consumer --bootstrap-server localhost:9092 --topic game.broadcast --from-beginning --timeout-ms 6000 2>/dev/null"

$gameOverCount = 0
if ($kafkaLogs) {
    $gameOverCount = ($kafkaLogs | Select-String -Pattern "GameOver" | Measure-Object).Count
}

# ── SONUC ─────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "================ SONUC ================" -ForegroundColor Cyan
Write-Host "  game.broadcast icindeki GameOver mesaji sayisi: $gameOverCount" -ForegroundColor White
Write-Host "=======================================" -ForegroundColor Cyan

if ($gameOverCount -eq 1) {
    Write-Host ""
    Write-Host "  OK! EXACTLY-ONCE DOGRULANDI!" -ForegroundColor Green
    Write-Host "  Lider ($leader) coktugunde diger sunucular devralip GameOver'i" -ForegroundColor Green
    Write-Host "  tam 1 kez uretti. Ne kayip, ne duplikasyon." -ForegroundColor Green
} elseif ($gameOverCount -eq 0) {
    Write-Host ""
    Write-Host "  BASARISIZ! Mesaj kaybi (at-most-once)." -ForegroundColor Red
    Write-Host "  GameOver hic Kafka'ya dusmemis." -ForegroundColor Red
} else {
    Write-Host ""
    Write-Host "  BASARISIZ! Duplikasyon (at-least-once)." -ForegroundColor Red
    Write-Host "  GameOver $gameOverCount kez uretilmis." -ForegroundColor Red
}
Write-Host ""
