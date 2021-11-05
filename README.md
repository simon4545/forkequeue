# forkequeue

message queue for forke

usage:

docker build -t forkequeue:v1.0 .

docker run -d --restart=always --name forke-queue -p 8989:8989 -v /your-local-data-path:/data forkequeue:v1.0