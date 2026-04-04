#!/bin/bash
# DuckDB K8s Deployment Helper Script

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGISTRY="${REGISTRY:-duckdb-server}"
TAG="${TAG:-latest}"
NAMESPACE="${NAMESPACE:-default}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Display usage
usage() {
    cat << EOF
Usage: $0 <command> [options]

Commands:
    build               Build Docker image
    local-test          Run locally with Docker Compose
    deploy              Deploy to Kubernetes
    undeploy            Remove from Kubernetes
    logs                View pod logs
    status              Show deployment status
    port-forward        Setup port forwarding
    clean               Clean up resources
    help                Display this help message

Options:
    --registry REGISTRY Docker registry (default: duckdb-server)
    --tag TAG           Docker image tag (default: latest)
    --namespace NS      K8s namespace (default: default)

Examples:
    $0 build
    $0 local-test
    $0 deploy --namespace duckdb
    $0 logs --namespace duckdb
    $0 port-forward --namespace duckdb
EOF
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --registry)
            REGISTRY="$2"
            shift 2
            ;;
        --tag)
            TAG="$2"
            shift 2
            ;;
        --namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        *)
            COMMAND="$1"
            shift
            ;;
    esac
done

# Build Docker image
build() {
    log_info "Building Docker image: ${REGISTRY}:${TAG}"
    cd "$SCRIPT_DIR"
    docker build -f docker/Dockerfile -t "${REGISTRY}:${TAG}" .
    log_success "Docker image built successfully"
}

# Local testing with Docker Compose
local_test() {
    log_info "Starting local testing with Docker Compose"
    cd "$SCRIPT_DIR/docker"
    docker-compose up -d
    log_success "DuckDB server running at http://localhost:8000"
    log_info "API Documentation: http://localhost:8000/docs"
    echo ""
    echo "Useful commands:"
    echo "  Check health:  curl http://localhost:8000/health"
    echo "  List tables:   curl http://localhost:8000/tables"
    echo "  View logs:     docker-compose logs -f duckdb"
    echo "  Stop:          docker-compose down"
    cd - > /dev/null
}

# Deploy to Kubernetes
deploy() {
    log_info "Deploying DuckDB to Kubernetes namespace: ${NAMESPACE}"

    # Check if kubectl is available
    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl not found. Please install kubectl."
        exit 1
    fi

    # Create namespace if it doesn't exist
    if kubectl get namespace "$NAMESPACE" &> /dev/null; then
        log_info "Using existing namespace: $NAMESPACE"
    else
        log_info "Creating namespace: $NAMESPACE"
        kubectl create namespace "$NAMESPACE"
    fi

    # Apply manifests
    log_info "Applying ConfigMap"
    kubectl apply -f "$SCRIPT_DIR/k8s/duckdb-configmap.yaml" -n "$NAMESPACE"

    log_info "Applying PersistentVolume and PersistentVolumeClaim"
    kubectl apply -f "$SCRIPT_DIR/k8s/duckdb-pvc.yaml" -n "$NAMESPACE"

    log_info "Applying Deployment"
    kubectl apply -f "$SCRIPT_DIR/k8s/duckdb-deployment.yaml" -n "$NAMESPACE"

    log_info "Applying Services"
    kubectl apply -f "$SCRIPT_DIR/k8s/duckdb-service.yaml" -n "$NAMESPACE"

    log_success "Deployment completed"
    log_info "Waiting for pod to be ready (this may take a minute)..."

    kubectl wait --for=condition=ready pod -l app=duckdb -n "$NAMESPACE" --timeout=300s || true

    log_success "DuckDB deployed successfully!"
    echo ""
    echo "Next steps:"
    echo "  Check status:   $0 status --namespace $NAMESPACE"
    echo "  View logs:      $0 logs --namespace $NAMESPACE"
    echo "  Port forward:   $0 port-forward --namespace $NAMESPACE"
}

# Undeploy from Kubernetes
undeploy() {
    log_warning "Removing DuckDB from Kubernetes namespace: ${NAMESPACE}"
    read -p "Are you sure? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        kubectl delete -f "$SCRIPT_DIR/k8s/duckdb-service.yaml" -n "$NAMESPACE" --ignore-not-found
        kubectl delete -f "$SCRIPT_DIR/k8s/duckdb-deployment.yaml" -n "$NAMESPACE" --ignore-not-found
        kubectl delete -f "$SCRIPT_DIR/k8s/duckdb-pvc.yaml" -n "$NAMESPACE" --ignore-not-found
        kubectl delete -f "$SCRIPT_DIR/k8s/duckdb-configmap.yaml" -n "$NAMESPACE" --ignore-not-found
        log_success "DuckDB undeployed"
    else
        log_info "Operation cancelled"
    fi
}

# View logs
logs() {
    log_info "Streaming logs from DuckDB pods in namespace: ${NAMESPACE}"
    kubectl logs -l app=duckdb -n "$NAMESPACE" -f --tail=50
}

# Show deployment status
status() {
    log_info "Deployment Status in namespace: ${NAMESPACE}"
    echo ""

    echo "Pods:"
    kubectl get pods -l app=duckdb -n "$NAMESPACE"
    echo ""

    echo "Services:"
    kubectl get svc -l app=duckdb -n "$NAMESPACE"
    echo ""

    echo "PersistentVolumeClaims:"
    kubectl get pvc -n "$NAMESPACE" -l app=duckdb || echo "None found"
    echo ""

    echo "Deployment:"
    kubectl get deployment -l app=duckdb -n "$NAMESPACE"
}

# Setup port forwarding
port_forward() {
    log_info "Setting up port forwarding for DuckDB service in namespace: ${NAMESPACE}"
    kubectl port-forward svc/duckdb-service 8000:8000 -n "$NAMESPACE"
}

# Clean up
clean() {
    log_warning "Cleaning up all DuckDB resources"
    read -p "Are you sure? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        log_info "Stopping local containers"
        cd "$SCRIPT_DIR/docker"
        docker-compose down || true
        cd - > /dev/null

        log_info "Removing DuckDB from Kubernetes"
        undeploy

        log_success "Cleanup completed"
    else
        log_info "Operation cancelled"
    fi
}

# Main
case "${COMMAND}" in
    build)
        build
        ;;
    local-test|local)
        build
        local_test
        ;;
    deploy)
        deploy
        ;;
    undeploy|remove)
        undeploy
        ;;
    logs)
        logs
        ;;
    status)
        status
        ;;
    port-forward|forward)
        port_forward
        ;;
    clean)
        clean
        ;;
    help|--help|-h)
        usage
        ;;
    *)
        log_error "Unknown command: ${COMMAND}"
        echo ""
        usage
        exit 1
        ;;
esac
