# forkequeue

message queue for forke

usage:

docker build -t forkequeue:v1.0 .

docker run -d --restart=always --name forke-queue -p 8989:8989 -v /your-local-data-path:/data forkequeue:v1.0

## api文档:

### 消息入队:

http://domain-name/api/queue/push?topic=your-topic-name  
POST Content-Type: application/json   
{"data":your-msg}

SUCCESS RESPONSE: {"code":200,"data":{},"msg":"SUCCESS"}

### 消息出队 :

http://domain-name/api/queue/pop?topic=your-topic-name  
POST Content-Type: application/json

SUCCESS RESPONSE: {"code":200,"data":{"id":msg-id,"data":your-msg},"msg":"SUCCESS"}

example response:{"code":200,"data":{"id":[48,102,99,99,102,97,55,52,48,53,99,48,49,48,48,100],"data":{"name":"李白","
age":33}},"msg":"SUCCESS"}

### 消息ACK:
消息出队后有60s时间Ack确认,如果超过60s未Ack成功，则消息会重新入队

http://domain-name/api/queue/ack?topic=your-topic-name  
POST Content-Type: application/json   
{"id":msg-id}  
post data example:{"id":[48,102,99,99,102,97,55,52,48,53,99,48,49,48,48,100]}

SUCCESS RESPONSE: {"code":200,"data":{},"msg":"SUCCESS"}

### 消息队列状态:

http://domain-name/api/queue/stats?topic=your-topic-name

不携带topic名称则查询所有topic状态

POST Content-Type: application/json

example response: {"code":200,"data":{"start_time":1636102436,"topics":[{"topic_name":"test","message_count":100,"in_ack_count":10}]},"msg":"SUCCESS"}