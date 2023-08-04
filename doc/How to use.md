## Build

1. clone nodex repo  
`git clone `
2. generate protobuf file  
`protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative pkg/pb/*.proto`
3. build s3proxy  
`go build -o s3 ./cmd/s3`
4. build remotedb  
`go build -o remote ./cmd/remote`
5. build ndrc  
`go build -o ndrc ./cmd/ndrc`
6. clone this repo   
[Title](https://github.com/DeBankDeFi/go-ethereum-debank)
7. build geth for nodex   
`make geth`

## Deploy
need s3 and kafka

1. deploy s3proxy  
`AWS_REGION={aws region},AWS_ACCESS_KEY_ID={aws s3 acees key},AWS_SECRET_ACCESS_KEY={aws s3 secert} ./s3`

2. deploy ndrc  
`./ndrc daemon`

3. deploy remotedb
/etc/eth/config.json
```
    {
      "db_infos": [
          {
              "id": 0,
              "db_type": "leveldb",
              "db_path": "/eth_readonly/geth/chaindata",
              "is_meta": true
          }
      ]
    } 
```

```
./remotedb -kafka_addr kafka:9092 -s3proxy_addr s3-proxy:8765  -listen_addr 0.0.0.0:8654 -db_cache_size 3072  -db_info_path /etc/eth/config.json  -env prod -chain_id eth -role master -ndrc_addrs ndrc:8089
```

4. deploy write node
add nodex config to geth's config.toml
```
   [Eth.Rpl]
    IsWriter = true
    S3ProxyAddr = "s3-proxy:8765"
    KafkaAddr = "kafka:9092"
    ChainId = "eth"
    Env = "prod"
    Role = "master"
    ReorgDeep = 128
```

```
geth \
--config /etc/eth/config.toml \
--syncmode=full \
--cache 2048 \
--cache.snapshot 50 \
--gcmode=archive \
--datadir $DATA_DIR --ancient.prune=true \
--http --http.addr=0.0.0.0 --http.port 8545 \
--http.api net,web3,eth,admin,txpool,pre,engine
```

5. deploy read geth
add nodex config to geth's config.toml
```
    [Eth.Rpl]
    IsWriter = false
    RemoteAddr = "remotedb:8654"
```

```
geth \
--config /etc/eth/config.toml \
--syncmode=full \
--cache 2048 \
--cache.snapshot 50 \
--gcmode=archive \
--datadir $DATA_DIR --ancient.prune=true \
--http --http.addr=0.0.0.0 --http.port 8545 \
--http.api net,web3,eth,admin,txpool,pre,engine
```