package util

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"strings"
)

type ResponseObject struct {
	ErrCode int         `json:"code"`
	ErrMsg  string      `json:"msg"`
	Result  interface{} `json:"data"`
}

func (this *ResponseObject) writeResponse(c *gin.Context) {
	c.JSON(200, this)
}

func (this *ResponseObject) WriteErrorMsg(c *gin.Context, message string, errCode int) {
	this.ErrCode = errCode
	this.ErrMsg = message
	this.writeResponse(c)
}

func (this *ResponseObject) WriteWithError(c *gin.Context, errCode int, msg string, errors ...error) {
	this.WriteErrorMsg(c, fmt.Sprint(msg, ` errors message:`, errors), errCode)
}

func (this *ResponseObject) WriteSuccessResult(c *gin.Context, entity interface{}) {
	this.ErrCode = 0
	this.Result = entity
	this.writeResponse(c)
}

func HTTPSuccess(c *gin.Context, entity interface{}) {
	responseObj := ResponseObject{Result: entity}
	responseObj.writeResponse(c)
}

func HTTPWriteWithError(c *gin.Context, errCode int, msg string, errors ...error) {
	HTTPWriteWithErrMsg(c, errCode, fmt.Sprint(msg, ` errors message:`, errors))
}

func HTTPWriteWithErrMsg(c *gin.Context, errCode int, msg string) {
	responseObj := ResponseObject{ErrCode: errCode, ErrMsg: msg}
	responseObj.writeResponse(c)
}

func CheckJWTHasTheRole(currentPath string, userRoles []string, path []RolePath) bool {
	for m := 0; m < len(userRoles); m++ {
		currentRole := userRoles[m]
		for i := 0; i < len(path); i++ {
			crtRolePath := path[i]
			if strings.TrimSpace(crtRolePath.Role) == strings.TrimSpace(currentRole) {
				curRoleSettingPath := crtRolePath.Path
				for k := 0; k < len(curRoleSettingPath); k++ {
					settingPathItemArray := strings.Split(curRoleSettingPath[k], ",")
					if MatchMultipleSimple(settingPathItemArray, currentPath) {
						return true
					}
				}
			}
		}
	}
	return false
}
