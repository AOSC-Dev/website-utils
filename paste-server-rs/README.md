# paste-server-rs

[![paste-server-rs](https://github.com/AOSC-Dev/website-2023-utils/actions/workflows/paste-server-rs.yml/badge.svg?branch=master)](https://github.com/AOSC-Dev/website-2023-utils/actions/workflows/paste-server-rs.yml)

## Develop (with docker)

Build:
```shell
docker compose up --build
```

Test:
```shell
echo "test" | curl -F "c=@-" http://localhost:2334/ | jq
```

## Deploy

At `/opt/paste-server-rs/`:
- Copy or link the `production.compose.yml` to `compose.yml`
- Create a `.env` file
- Run `docker compose up -d`

`.env` file example:
```
POSTGRES_USER=postgres
POSTGRES_PASSWORD=somepassword
POSTGRES_DB=paste
```
