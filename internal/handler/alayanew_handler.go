package handler

import (
	"strconv"
	"strings"

	"dingoscheduler/internal/model/query"
	"dingoscheduler/internal/service"
	myerr "dingoscheduler/pkg/error"
	"dingoscheduler/pkg/util"

	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

type AlayanewHandler struct {
	repositoryService *service.RepositoryService
	tagService        *service.TagService
}

func NewAlayanewHandler(repositoryService *service.RepositoryService, tagService *service.TagService) *AlayanewHandler {
	return &AlayanewHandler{
		repositoryService: repositoryService,
		tagService:        tagService,
	}
}

func (handler *AlayanewHandler) RepositoriesHandler(c echo.Context) error {
	name := c.QueryParam("name")
	instanceId := c.QueryParam("instanceId")
	var (
		page, pageSize int
		err            error
	)
	if page, err = extractPageParam(c, "page"); err != nil {
		return util.ErrorRequestParam(c)
	}
	if pageSize, err = extractPageParam(c, "pageSize"); err != nil {
		return util.ErrorRequestParam(c)
	}
	sortBy := extractStringParam(c, "sort", "last_modified")
	sortDir := extractStringParam(c, "order", "desc")
	sortDir = strings.ToLower(sortDir)
	if sortDir != "asc" && sortDir != "desc" {
		return util.ErrorRequestParam(c)
	}
	license := c.QueryParam("license")
	library := c.QueryParam("library")
	language := c.QueryParam("language")
	pipelineTag := c.QueryParam("pipeline_tag")
	apps := c.QueryParam("apps")
	inferenceProvider := c.QueryParam("inference_provider")
	other := c.QueryParam("other")
	models, total, err := handler.repositoryService.RepositoryList(&query.ModelQuery{
		InstanceId:        instanceId,
		Name:              name,
		Page:              page,
		PageSize:          pageSize,
		Sort:              sortBy,
		Order:             sortDir,
		PipelineTag:       pipelineTag,
		Library:           library,
		Apps:              apps,
		InferenceProvider: inferenceProvider,
		Language:          language,
		License:           license,
		Other:             other,
	})
	if err != nil {
		zap.S().Errorf("RepositoryList err.%v", err)
		if e, ok := err.(myerr.Error); ok {
			return util.ErrorEntryUnknown(c, e.StatusCode(), err.Error())
		}
		return util.ResponseError(c)
	}
	return util.ResponseData(c, util.PageData{
		Total: total,
		List:  models,
	})
}

func extractPageParam(c echo.Context, pageParamName string) (int, error) {
	pageStr := c.QueryParam(pageParamName)
	if pageStr == "" {
		pageStr = "0"
	}
	page, err := strconv.Atoi(pageStr)
	if err != nil {
		zap.S().Errorf("param conv err.%v", err)
		return 0, util.ErrorRequestParam(c)
	}
	return page, nil
}

func extractStringParam(c echo.Context, pageParamName, defaultValue string) string {
	value := c.QueryParam(pageParamName)
	if value == "" {
		value = defaultValue
	}
	return value
}

func (handler *AlayanewHandler) RepositoryInfoHandler(c echo.Context) error {
	id := util.Atoi64(c.Param("id"))
	model, err := handler.repositoryService.GetRepositoryById(id)
	if err != nil {
		zap.S().Errorf("GetRepositoryById err.%v", err)
		if e, ok := err.(myerr.Error); ok {
			return util.ErrorEntryUnknown(c, e.StatusCode(), e.Error())
		}
		return util.ResponseError(c)
	}
	return util.ResponseData(c, model)
}

func (handler *AlayanewHandler) RepositoryCardHandler(c echo.Context) error {
	instanceId := c.Param("instanceId")
	id := util.Atoi64(c.Param("id"))
	err := handler.repositoryService.RepositoryCardById(c, instanceId, id)
	if err != nil {
		zap.S().Errorf("GetRepositoryById err.%v", err)
		return util.ResponseError(c)
	}
	return nil
}

func (handler *AlayanewHandler) RepositoryFilesHandler(c echo.Context) error {
	instanceId := c.Param("instanceId")
	id := util.Atoi64(c.Param("id"))
	filePath := c.Param("filePath")
	err := handler.repositoryService.RepositoryFilesById(c, instanceId, id, filePath)
	if err != nil {
		zap.S().Errorf("GetRepositoryById err.%v", err)
		return util.ResponseError(c)
	}
	return nil
}

func (handler *AlayanewHandler) TagHandler(c echo.Context) error {
	tagTypesStr := c.QueryParam("type")
	tagSubTypeStr := c.QueryParam("subType")

	var tagTypes []string
	if tagTypesStr != "" {
		tagTypesStr = strings.TrimSpace(tagTypesStr)
		parts := strings.Split(tagTypesStr, ",")
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				tagTypes = append(tagTypes, trimmed)
			}
		}
	}

	var tagSubTypes []string
	if tagSubTypeStr != "" {
		tagSubTypeStr = strings.TrimSpace(tagSubTypeStr)
		parts := strings.Split(tagSubTypeStr, ",")
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				tagSubTypes = append(tagSubTypes, trimmed)
			}
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

func (handler *AlayanewHandler) TaskTagHandler(c echo.Context) error {
	taskTags, err := handler.tagService.TaskTagList(&query.TagQuery{
		Types: []string{"pipeline_tag"},
	})
	if err != nil {
		if e, ok := err.(myerr.Error); ok {
			return util.ErrorEntryUnknown(c, e.StatusCode(), e.Error())
		}
		return util.ErrorProxyError(c)
	}
	return util.ResponseData(c, taskTags)
}

func (handler *AlayanewHandler) MainTagHandler(c echo.Context) error {
	mainTags, err := handler.tagService.MainTagList()
	if err != nil {
		if e, ok := err.(myerr.Error); ok {
			return util.ErrorEntryUnknown(c, e.StatusCode(), e.Error())
		}
		return util.ErrorProxyError(c)
	}
	return util.ResponseData(c, mainTags)
}
