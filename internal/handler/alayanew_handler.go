package handler

import (
	"encoding/json"
	"strconv"
	"strings"

	"dingoscheduler/internal/model/query"
	"dingoscheduler/internal/service"
	myerr "dingoscheduler/pkg/error"
	"dingoscheduler/pkg/util"

	"github.com/labstack/echo/v4"
)

type AlayanewHandler struct {
	repositoryService *service.RepositoryService
}

func NewAlayanewHandler(repositoryService *service.RepositoryService) *AlayanewHandler {
	return &AlayanewHandler{
		repositoryService: repositoryService,
	}
}

func (handler *AlayanewHandler) ModelsHandler(c echo.Context) error {
	name := c.QueryParam("name")
	instanceId := c.QueryParam("instanceId")
	page, _ := strconv.Atoi(c.QueryParam("page"))
	if page < 1 {
		page = 1
	}
	pageSize, _ := strconv.Atoi(c.QueryParam("pageSize"))
	if pageSize < 1 || pageSize > 100 {
		pageSize = 10
	}
	sortBy := c.QueryParam("sort")
	if sortBy == "" {
		sortBy = "last_modified"
	}
	sortDir := c.QueryParam("order")
	if sortDir == "" {
		sortDir = "desc"
	}
	sortDir = strings.ToLower(sortDir)
	if sortDir != "asc" && sortDir != "desc" {
		return util.ErrorRequestParam(c)
	}
	models, total, err := handler.repositoryService.ModelList(&query.ModelQuery{
		InstanceId: instanceId,
		Name:       name,
		Page:       page,
		PageSize:   pageSize,
		Sort:       sortBy,
		Order:      sortDir,
	})
	if err != nil {
		if e, ok := err.(myerr.Error); ok {
			return util.ErrorEntryUnknown(c, e.StatusCode(), e.Error())
		}
		return util.ErrorProxyError(c)
	}
	return util.ResponseData(c, util.PageData{
		Total: total,
		List:  models,
	})
}

func (handler *AlayanewHandler) ModelInfoHandler(c echo.Context) error {
	id := util.Atoi64(c.Param("id"))
	models, err := handler.repositoryService.GetById(id)
	if err != nil {
		if e, ok := err.(myerr.Error); ok {
			return util.ErrorEntryUnknown(c, e.StatusCode(), e.Error())
		}
		return util.ErrorProxyError(c)
	}
	return util.ResponseData(c, models)
}

func (handler *AlayanewHandler) TagHandler(c echo.Context) error {
	tagTypesStr := c.QueryParam("type")
	tagSubTypeStr := c.QueryParam("subType")

	var tagTypes []string
	if tagTypesStr != "" {
		tagTypesStr = strings.TrimSpace(tagTypesStr)
		if err := json.Unmarshal([]byte(tagTypesStr), &tagTypes); err != nil {
			return util.ErrorEntryUnknown(c, 400, "type 参数格式错误，应为 JSON 数组字符串")
		}
	}

	var tagSubTypes []string
	if tagSubTypeStr != "" {
		tagSubTypeStr = strings.TrimSpace(tagSubTypeStr)
		if err := json.Unmarshal([]byte(tagSubTypeStr), &tagSubTypes); err != nil {
			return util.ErrorEntryUnknown(c, 400, "subType 参数格式错误，应为 JSON 数组字符串")
		}
	}

	tags, err := handler.tagService.TagListByCondition(&query.TagQuery{
		Types:    tagTypes,
		SubTypes: tagSubTypes,
	})
	if err != nil {
		if e, ok := err.(myerr.Error); ok {
			return util.ErrorEntryUnknown(c, e.StatusCode(), e.Error())
		}
		return util.ErrorProxyError(c)
	}
	return util.ResponseData(c, tags)
}
