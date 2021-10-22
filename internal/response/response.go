package response

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

type Response struct {
	Code int         `json:"code"` //错误码((200:成功, 300:失败, 400:身份验证失败))
	Data interface{} `json:"data"` //返回数据(业务接口定义具体数据结构)
	Msg  string      `json:"msg"`  //提示信息
}

const (
	SUCCESS      = 200
	ERROR        = 300
	AuthFailCode = 400
)

func Result(code int, data interface{}, msg string, c *gin.Context) {
	// 开始时间
	c.JSON(http.StatusOK, Response{
		code,
		data,
		msg,
	})
}

func Ok(c *gin.Context) {
	Result(SUCCESS, map[string]interface{}{}, "SUCCESS", c)
}

func OkWithMessage(message string, c *gin.Context) {
	Result(SUCCESS, map[string]interface{}{}, message, c)
}

func OkWithData(data interface{}, c *gin.Context) {
	Result(SUCCESS, data, "SUCCESS", c)
}

func OkWithDetailed(data interface{}, message string, c *gin.Context) {
	Result(SUCCESS, data, message, c)
}

func AuthFail(c *gin.Context) {
	Result(AuthFailCode, map[string]interface{}{}, "LOGIN_INVALID", c)
}

func Fail(c *gin.Context) {
	Result(ERROR, map[string]interface{}{}, "FAIL", c)
}

func FailWithMessage(message string, c *gin.Context) {
	Result(ERROR, map[string]interface{}{}, message, c)
}

func FailWithDetailed(data interface{}, message string, c *gin.Context) {
	Result(ERROR, data, message, c)
}
