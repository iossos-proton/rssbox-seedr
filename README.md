# RSSBOX SEEDR  

RSSBOX SEEDR is a simple, lightweight utility for downloading files from RSS feeds.

## Installation

### Docker

Run the following command to start the container:
```shell
docker build -t rssbox .
docker run -d rssbox
```

### Docker Compose

Run the following command to start the container:
```shell
docker-compose up -d
```

### From Source

Clone the repository and install the required dependencies:

```shell
cd rssbox-seedr
pip install -r requirements.txt
python -m rssbox
```

## Configuration

Edit the `.env` file to configure the application:

- `RSS_URL`: The URL of the RSS feed to download.
- `DETA_KEY`: The API key for the DETA API.
- `MONGO_URL`: The URL of the MongoDB database.

## License

This project is licensed under the GNU General Public License v3.0. See the [LICENSE](./LICENSE) file for more information.