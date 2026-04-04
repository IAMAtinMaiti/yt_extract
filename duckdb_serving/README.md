# DuckDB Server - Kubernetes Deployment

This folder contains the necessary configuration and code to deploy DuckDB as a scalable, persistent service on Kubernetes with RESTful API access.

## Features

- **RESTful API**: FastAPI-based REST endpoint for querying DuckDB
- **Persistent Storage**: PersistentVolumeClaim (PVC) for data persistence across pod restarts
- **Kubernetes Ready**: Production-ready K8s manifests and configurations
- **Health Checks**: Liveness and readiness probes for container orchestration
- **Docker Support**: Dockerfile for containerization and docker-compose for local testing
- **Data Initialization**: Automated schema creation with YouTube trending data tables

## Directory Structure

```
duckdb_serving/
├── api/                    # Python API server
│   ├── __init__.py
│   └── duckdb_server.py   # FastAPI application
├── k8s/                    # Kubernetes manifests
│   ├── duckdb-deployment.yaml   # K8s Deployment
│   ├── duckdb-service.yaml      # K8s Services (ClusterIP + LoadBalancer)
│   ├── duckdb-pvc.yaml          # PersistentVolume and PersistentVolumeClaim
│   └── duckdb-configmap.yaml    # ConfigMap for environment variables
├── init/                   # Database initialization scripts
│   ├── init_db.py         # Python initialization script
│   └── init_db.sh         # Shell initialization script
├── docker/                 # Docker configuration
│   ├── Dockerfile         # Docker image definition
│   └── docker-compose.yaml # Docker Compose for local testing
├── requirements.txt       # Python dependencies
└── README.md             # This file
```

## Quick Start

### Local Testing with Docker Compose

1. **Build and run the container**:
   ```bash
   cd docker
   docker-compose up -d
   ```

2. **Access the API**:
   ```bash
   # Health check
   curl http://localhost:8000/health
   
   # Get all tables
   curl http://localhost:8000/tables
   
   # Execute a query
   curl -X POST http://localhost:8000/query \
     -H "Content-Type: application/json" \
     -d '{"sql": "SELECT * FROM youtube_categories LIMIT 5"}'
   ```

3. **View API documentation**:
   Open browser and navigate to: `http://localhost:8000/docs`

4. **Stop the container**:
   ```bash
   cd docker
   docker-compose down
   ```

### Kubernetes Deployment

1. **Create namespace (optional)**:
   ```bash
   kubectl create namespace duckdb
   ```

2. **Apply configuration files** (in order):
   ```bash
   # Apply ConfigMap
   kubectl apply -f k8s/duckdb-configmap.yaml
   
   # Apply PersistentVolume and PersistentVolumeClaim
   kubectl apply -f k8s/duckdb-pvc.yaml
   
   # Apply Deployment
   kubectl apply -f k8s/duckdb-deployment.yaml
   
   # Apply Services
   kubectl apply -f k8s/duckdb-service.yaml
   ```

3. **Verify deployment**:
   ```bash
   # Check pod status
   kubectl get pods -l app=duckdb
   
   # Check service status
   kubectl get svc -l app=duckdb
   
   # View pod logs
   kubectl logs -l app=duckdb -f
   
   # Describe deployment
   kubectl describe deployment duckdb-server
   ```

4. **Access the service**:
   ```bash
   # Port forward to localhost (default)
   kubectl port-forward svc/duckdb-service 8000:8000
   
   # Or use LoadBalancer (if available)
   kubectl get svc duckdb-service-lb
   ```

5. **Query the API**:
   ```bash
   curl http://localhost:8000/health
   ```

6. **Clean up**:
   ```bash
   # Delete all DuckDB resources
   kubectl delete -f k8s/duckdb-service.yaml
   kubectl delete -f k8s/duckdb-deployment.yaml
   kubectl delete -f k8s/duckdb-pvc.yaml
   kubectl delete -f k8s/duckdb-configmap.yaml
   
   # Or delete entire namespace
   kubectl delete namespace duckdb
   ```

## API Endpoints

### Health & Status

- **GET /health** - Health check endpoint
- **GET /ready** - Readiness check endpoint
- **GET /stats** - Database statistics

### Database Operations

- **POST /query** - Execute SELECT queries (returns data)
  ```json
  {
    "sql": "SELECT * FROM table_name LIMIT 10"
  }
  ```

- **POST /execute** - Execute statements (INSERT, UPDATE, DELETE, CREATE)
  ```json
  {
    "sql": "INSERT INTO table_name VALUES (...)"
  }
  ```

### Schema Information

- **GET /tables** - List all tables in database
- **GET /schema/{table_name}** - Get schema details for a table

### Example Usage

```bash
# Query YouTube videos
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT channel_title, COUNT(*) as video_count, AVG(views) as avg_views FROM youtube_videos GROUP BY channel_title ORDER BY video_count DESC LIMIT 10"
  }'

# Get trending summary
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM trending_summary WHERE snapshot_date = CURRENT_DATE"
  }'
```

## Database Schema

### youtube_videos
Stores YouTube video metadata and metrics.

| Column | Type | Description |
|--------|------|-------------|
| video_id | VARCHAR PRIMARY KEY | Unique video identifier |
| title | VARCHAR | Video title |
| channel_id | VARCHAR | Channel identifier |
| channel_title | VARCHAR | Channel name |
| category_id | INTEGER | Video category |
| publish_time | TIMESTAMP | Publication timestamp |
| tags | VARCHAR | Video tags |
| views | INTEGER | View count |
| likes | INTEGER | Like count |
| comment_count | INTEGER | Comment count |
| thumbnail_link | VARCHAR | Thumbnail URL |
| comments_disabled | BOOLEAN | Comments disabled flag |
| ratings_disabled | BOOLEAN | Ratings disabled flag |
| snapshot_date | DATE | Date when data was captured |

### youtube_categories
Reference table for YouTube categories.

| Column | Type | Description |
|--------|------|-------------|
| category_id | INTEGER PRIMARY KEY | Category identifier |
| category_name | VARCHAR | Category name |
| snippet_title | VARCHAR | Category description |

### trending_summary (View)
Aggregated trending statistics.

## Environment Variables

- `DUCKDB_DATA_PATH` - Path to DuckDB database file (default: `/data/duckdb/data.duckdb`)
- `LOG_LEVEL` - Logging level: DEBUG, INFO, WARNING, ERROR (default: `INFO`)

## Storage Configuration

### Local Development
Uses hostPath storage for local K8s testing.

### Production
For production deployments, replace the PVC configuration with your cloud provider's storage class:

**AWS EBS**:
```yaml
storageClassName: ebs-sc
```

**Google Cloud Persistent Disk**:
```yaml
storageClassName: pd-standard
```

**Azure**:
```yaml
storageClassName: managed-premium
```

## Performance Considerations

1. **Connection Pooling**: DuckDB uses a single connection per process; scaling horizontally requires read replicas or additional instances
2. **Indices**: Pre-created indices on frequently queried columns (channel_id, category_id, snapshot_date)
3. **Query Optimization**: DuckDB automatically optimizes queries; use EXPLAIN to analyze query plans
4. **Storage Size**: Monitor PVC usage; default allocated 10Gi
5. **Memory**: Current limits set to 1Gi; adjust based on query complexity

## Troubleshooting

### Pod not starting
```bash
# Check pod logs
kubectl logs duckdb-server

# Check events
kubectl describe pod duckdb-server
```

### Database file corrupted
```bash
# Delete PVC and reinitialize
kubectl delete pvc duckdb-pvc
kubectl apply -f k8s/duckdb-pvc.yaml
kubectl rollout restart deployment duckdb-server
```

### Connection timeouts
```bash
# Check service connectivity
kubectl exec -it duckdb-server -- curl http://localhost:8000/health

# Check DNS
kubectl exec -it duckdb-server -- nslookup duckdb-service
```

### Storage issues
```bash
# Check PVC status
kubectl get pvc

# Check node storage
kubectl describe nodes

# Check PV status
kubectl get pv
```

## Building Docker Image

```bash
# Build for local use
cd docker
docker build -t duckdb-server:latest .

# Tag for registry
docker tag duckdb-server:latest your-registry/duckdb-server:latest

# Push to registry
docker push your-registry/duckdb-server:latest
```

## Advanced Configuration

### Increasing Resources
Edit `duckdb-deployment.yaml`:
```yaml
resources:
  requests:
    memory: "1Gi"
    cpu: "500m"
  limits:
    memory: "4Gi"
    cpu: "2000m"
```

### Horizontal Pod Autoscaling
```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: duckdb-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: duckdb-server
  minReplicas: 1
  maxReplicas: 5
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
```

### Ingress Configuration
```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: duckdb-ingress
spec:
  rules:
  - host: duckdb.example.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: duckdb-service
            port:
              number: 8000
```

## Security Best Practices

1. **Network Policies**: Restrict traffic to DuckDB service
2. **RBAC**: Limit pod permissions
3. **Resource Limits**: Prevent resource exhaustion attacks
4. **Read-Only Filesystem**: Consider running with read-only root filesystem
5. **Security Context**: Run as non-root user
6. **Secrets Management**: Use K8s Secrets for sensitive data

## License

Same as parent project

## Support

For issues or questions, refer to:
- [DuckDB Documentation](https://duckdb.org/docs/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Kubernetes Documentation](https://kubernetes.io/docs/)

