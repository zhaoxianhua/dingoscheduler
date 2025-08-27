package service

import (
	"dingoscheduler/internal/dao"
	"dingoscheduler/internal/model/dto"
	"dingoscheduler/internal/model/query"

	"github.com/young2j/gocopy"
)

type TagService struct {
	tagDao *dao.TagDao
}

func NewTagService(tagDao *dao.TagDao) *TagService {
	return &TagService{
		tagDao: tagDao,
	}
}

func (t *TagService) TagListByCondition(query *query.TagQuery) ([]*dto.Tag, error) {
	// 调用DAO层的ListByCondition方法查询标签
	tags, err := t.tagDao.TagListByCondition(query)
	if err != nil {
		return nil, err
	}

	// 转换为DTO对象
	tagDTOs := make([]*dto.Tag, 0, len(tags))
	gocopy.Copy(&tagDTOs, &tags)

	return tagDTOs, nil
}
