FROM golang:alpine as build

# 容器环境变量添加，会覆盖默认的变量值
ENV GO111MODULE=on
ENV GOPROXY=https://goproxy.cn,direct

# 设置工作区
WORKDIR /go/release

# 把全部文件添加到/go/release目录
ADD . .

RUN GOOS=linux CGO_ENABLED=0 GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o forke-queue boot/boot.go

# 运行：使用alpine作为基础镜像
FROM alpine:latest as prod

ENV GIN_MODE=release
RUN mkdir -p /data
# 在build阶段复制可执行的go二进制文件app
COPY --from=build /go/release/forke-queue /

# 启动服务
CMD ["/forke-queue"]