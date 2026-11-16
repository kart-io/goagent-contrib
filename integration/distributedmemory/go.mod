module github.com/kart-io/goagent-contrib/integration/distributedmemory

go 1.27.0

require (
	github.com/jackc/pgx/v5 v5.10.0
	github.com/kart-io/goagent v0.53.0
	github.com/kart-io/goagent-contrib/cache/redis v0.0.0-00010101000000-000000000000
	github.com/kart-io/goagent-contrib/durable/pgstore v0.5.1-0.20261107151427-7637a02d2a30
	github.com/kart-io/goagent-contrib/memory/pgvector v0.0.0-00010101000000-000000000000
	github.com/redis/go-redis/v9 v9.21.0
	github.com/stretchr/testify v1.12.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/gofrs/flock v0.13.0 // indirect
	github.com/google/jsonschema-go v0.4.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/pgvector/pgvector-go v0.4.1 // indirect
	go.opentelemetry.io/otel v1.47.0 // indirect
	go.opentelemetry.io/otel/trace v1.47.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
)

// 仅测试模块、不发布：同仓库模块一律用本地源码，与 management 引用 durable/pgstore 的方式相同。
replace (
	github.com/kart-io/goagent-contrib/cache/redis => ../../cache/redis
	github.com/kart-io/goagent-contrib/durable/pgstore => ../../durable/pgstore
	github.com/kart-io/goagent-contrib/memory/pgvector => ../../memory/pgvector
)
