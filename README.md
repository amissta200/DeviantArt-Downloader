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


Make sure you setup a deviant art app.

And in your DeviantArt app settings:

Add EXACTLY:
http://localhost:8080/callback