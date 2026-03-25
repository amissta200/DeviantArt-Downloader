Copy .env.example to .env in the same directory and populate accordingly.

Run in docker using:

```bash
docker compose down --rmi all --volumes --remove-orphans
docker compose up --build -d
```

Following rebuilds can be done via

```bash
docker compose up --build -d
```


TODO:
- If not mature use content source
- If mature use https://www.deviantart.com/api/v1/oauth2/deviation/download/{{deviation_id}}