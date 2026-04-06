# Build the image
docker build -t dashboard:latest ./dashboard

# Run the container
docker run -p 8088:8088 dashboard:latest