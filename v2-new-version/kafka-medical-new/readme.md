docker compose down -v
docker compose up --build

docker compose up -d


docker compose logs -f postgres
docker compose logs -f kafka
docker compose logs -f debezium
docker compose logs -f dashboard


curl -X POST \
  -H "Content-Type: application/json" \
  --data @debezium/postgres-connector.json \
  http://localhost:8084/connectors
