# Analytica - Gaming data analysis tool

## Tech stack used

- Python
- FastAPI
- Docker
- AWS EC2
- Clickhouse

## Setting up app locally

- Clone the repository

```
git clone  https://github.com/GaurKS/analytica.git
```

- Change directory to root and install all the packages in requirements.txt

```
cd analytica
pip download -r requirements.txt
```

- Create an environment file using the `.env.sample` reference
- Run the app and navigate to `localhost:8000/health`

```
uvicorn main:app --reload
```

## Running the app using docker image

- Make sure docker is downloaded
- Pull the docker image for the app

```
docker pull krugarr/fastapi-app-analytica
```

- Run the container

```
docker run -d -p 8000:8000 --name my-app krugarr/fastapi-app-analytica
```

- Navigate to `localhost:8000/health` to test the app

## Server endpoints

**GET**: `http://3.108.184.193/health`

- This is a simple health check endpoint to test the status of server. It is a public route

**GET**: `http://3.108.184.193/query`

- This is a query endpoint to query the data stored using multiple filters. It supports various filters, aggregation and search parameters. It is a protected route and can be accessed using a valid api-key.

**POST**: `http://3.108.184.193/upload`

- This is a post endpoint. It takes a csv file url as a body and stores the data in the database. File url can be passed as an `url` parameter in request body. A sample csv file url is attached below. Currently the endpoint only supports google drive download links which are accessible to public.

```
https://drive.google.com/uc?id=1ZIMlNQ226uaFvTId8ineajrBmWdQB2Ew&export=download
```

Complete details related to application endpoints, request body, query parameters and sample response can be found in the [API documentation](http://3.108.184.193/docs)

Docker Image is available on [DockerHub](https://hub.docker.com/repository/docker/krugarr/fastapi-app-analytica/general) 

All the deployed endpoints can be accessed at  `http://3.108.184.193/`

For deployment, T2.micro instance is used under AWS free cloud credits. For database, Clickhouse free cloud instance is used.
