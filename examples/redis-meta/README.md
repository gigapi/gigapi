![image](https://github.com/user-attachments/assets/0ca51072-1950-457c-81e6-109bba5ad6bb)

# <img src="https://github.com/user-attachments/assets/74a1fa93-5e7e-476d-93cb-be565eca4a59" height=24 />  GigAPI Redis Metadata Example

This example deploys GigAPI using Redis-compatible service for metadata storage

```yml
services:
  redis:
    image: redis:latest
    container_name: redis-server
    restart: unless-stopped
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    command: redis-server --appendonly yes

  gigapi:
    image: ghcr.io/gigapi/gigapi:latest
    container_name: gigapi
    hostname: gigapi
    restart: unless-stopped
    volumes:
      - ./data:/data
    ports:
      - "7971:7971"
      - "8082:8082"
    environment:
      - GIGAPI_ENABLED=true
      - GIGAPI_MERGE_TIMEOUT_S=10
      - GIGAPI_METADATA_TYPE=redis
      - GIGAPI_METADATA_URL=redis://redis:6379/0 
      - GIGAPI_ROOT=/data
      - PORT=7971
    depends_on:
      - redis

volumes:
  redis-data:
```
