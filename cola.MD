1. Sobe o kafka

```bash
docker compose up -d
```

2. Checa se subiu

```bash
docker compose ps
```

3. Cria os tópicos com replication 2

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh  --create --topic match-events-raw --partitions 3 --replication-factor 2 --bootstrap-server localhost:9092
```

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh  --create --topic match-insight --partitions 1 --replication-factor 2 --bootstrap-server localhost:9092
```

2. Mostra ele criado (kafka-get-offsets)

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092
```

3. Executa os consumidores antes


4. Mostra os grupos de consumidores


Comando para ver todos os grupos:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

Placar:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group placar-group
```

Finalizações:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group shots-group
```

Cartões:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group cards-group
```


5. executa todos os consumidores
6. roda o produtor
7. mostra os offsets dnv

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092
```

8. mostra os grupos novamente pra mostrar que eles consumiram tudo

Placar:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group placar-group
```

Finalizações:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group shots-group
```

Cartões:
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group cards-group
```

9. se quiser rodar dnv, deleta o tópico e reseta o offset dos grupos

Deletar tópico match-events-raw:

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic match-events-raw
```

Deletar tópico match-insights:

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic match-insight
```

Colocar offsets em 0


```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group placar-group --topic match-events-raw --reset-offsets --to-earliest --execute
```

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group shots-group --topic match-events-raw --reset-offsets --to-earliest --execute
```

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group cards-group --topic match-events-raw --reset-offsets --to-earliest --execute
```