# Ring of the Middle Earth

Tarayıcı üzerinden iki oyuncuyla oynanan, sıra tabanlı bir strateji oyunu ve onu çalıştıran olay güdümlü dağıtık sistemdir. Light Side, Ring Bearer'ı Mount Doom'a ulaştırmaya; Dark Side ise onu tespit edip durdurmaya çalışır.

Proje, Dağıtık Uygulama Geliştirme dersi kapsamında Go tabanlı oyun motoru seçeneğiyle hazırlanmıştır.

## Mimari

- Üç düğümlü Go oyun motoru
- Apache Kafka ve ZooKeeper
- Kafka Streams tabanlı doğrulama ve rota riski topolojileri
- Confluent Schema Registry ve Avro şemaları
- Nginx üzerinden sunulan HTML/CSS/JavaScript arayüzü
- Docker Compose ile yerel orkestrasyon

Oyun emirleri ve olaylar Kafka üzerinden akar. Light Side ve Dark Side için farklılaştırılmış görünüm, Ring Bearer konumunun karşı tarafa sızmasını engeller.

## Gereksinimler

- Docker Desktop veya Docker Engine
- Docker Compose v2
- Yalnızca testler için Go 1.22+

## Çalıştırma

```bash
docker compose up --build -d
```

Arayüzler:

- Light Side: http://localhost/?side=light
- Dark Side: http://localhost/?side=dark
- Alternatif port: http://localhost:8888

Logları izlemek için:

```bash
docker compose logs -f go-1 go-2 go-3 kafka-streams
```

Sistemi ve yerel volume'leri kapatmak için:

```bash
docker compose down -v
```

## Testler

Docker gerektirmeyen Go testleri:

```bash
cd option-b
go test ./...
```

Race detector ile:

```bash
cd option-b
go test -race ./...
```

## Proje yapısı

```text
.
├── config/        # Harita ve birim ayarları
├── kafka/         # Topic kurulumu, Avro şemaları ve Kafka Streams
├── option-b/      # Go oyun motoru ve testler
├── ui/            # Tarayıcı arayüzü
├── tools/         # Demo ve profil yardımcıları
├── docs/          # Mimari rapor, harita ve proje şartnamesi
└── docker-compose.yml
```

## Belgeler

- [Mimari rapor](docs/architecture.pdf)
- [Proje şartnamesi](docs/project-specification.md)
- [Orta Dünya haritası](docs/middle-earth-map.svg)

## Doğrulama durumu

Kaynaklar temizlenirken `go test ./...` komutu başarıyla tamamlanmıştır. Tam sistem testi Docker servislerinin çalışmasını gerektirir.
