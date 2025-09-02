//  Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http:www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package dao

import (
	"fmt"

	"dingoscheduler/internal/data"
	"dingoscheduler/internal/model"
	"dingoscheduler/internal/model/query"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

type RepositoryDao struct {
	baseData *data.BaseData
}

func NewRepositoryDao(data *data.BaseData) *RepositoryDao {
	return &RepositoryDao{
		baseData: data,
	}
}

func (d *RepositoryDao) Get(id int64) (*model.Repository, error) {
	var repository []*model.Repository
	if err := d.baseData.BizDB.Model(&model.Repository{}).Select("id, org_repo,like_num, download_num, pipeline_tag,last_modified,sha ").Where("id = ?", id).Find(&repository).Error; err != nil {
		return nil, err
	}
	if len(repository) > 0 {
		return repository[0], nil
	}
	return nil, fmt.Errorf("No record found")
}

func (d *RepositoryDao) RepoAndTagSave(repository *model.Repository, tags []*model.RepositoryTag) error {
	if err := d.baseData.BizDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(repository).Error; err != nil {
			return err
		}
		for i := range tags {
			tags[i].RepoId = repository.ID
		}
		if err := tx.Create(tags).Error; err != nil {
			return err
		}
		return nil
	}); err != nil {
		zap.S().Error("RepoAndTagSave err.%v", err)
		return err
	}
	return nil
}

func (d *RepositoryDao) GetFreeRepository(instanceId string) ([]*model.Repository, error) {
	var repositories []*model.Repository
	err := d.baseData.BizDB.Table("model_file_record t1").Select("distinct t1.datatype, t1.org, t1.repo ").
		Where(" t1.id in (SELECT x.record_id FROM dingo.model_file_process x where x.instance_id = ?) and t1.repo not in (select repo from repository where instance_id = ?)", instanceId, instanceId).Find(&repositories).Error
	return repositories, err
}

func (d *RepositoryDao) ModelList(query *query.ModelQuery) ([]*model.Repository, int64, error) {
	var repositories []*model.Repository
	db := d.baseData.BizDB.Model(&model.Repository{}).Select("id, org_repo,like_num, download_num, pipeline_tag, last_modified ")
	if query.InstanceId != "" {
		db.Where("instance_id = ?", query.InstanceId)
	}
	if query.Name != "" {
		db.Where(fmt.Sprintf("org_repo like '%s'", "%"+query.Name+"%"))
	}
	if query.PipelineTag != "" {
		db.Where("pipeline_tag_id = ?", query.PipelineTag)
	}
	tags := make([]string, 0)
	if query.Library != "" {
		tags = append(tags, query.Library)
	}
	if query.Apps != "" {
		tags = append(tags, query.Apps)
	}
	if query.InferenceProvider != "" {
		tags = append(tags, query.InferenceProvider)
	}
	if query.Language != "" {
		tags = append(tags, query.Language)
	}
	if query.License != "" {
		tags = append(tags, query.License)
	}
	if query.Other != "" {
		tags = append(tags, query.Other)
	}
	if len(tags) > 0 {
		db.Where(" id in (select repo_id from repository_tag where tag_id in (?))", tags)
	}
	var count int64
	if err := db.Count(&count).Error; err != nil {
		zap.S().Error("统计collect数量失败", err)
		return nil, 0, err
	}
	offset, pageSize := paginate(query.Page, query.PageSize)
	if query.Sort != "" && query.Order != "" {
		db.Order(fmt.Sprintf("%s %s offset %d limit %d", query.Sort, query.Order, offset, pageSize))
	} else {
		db.Order(fmt.Sprintf("offset %d limit %d", offset, pageSize))
	}
	err := db.Find(&repositories).Error
	return repositories, count, err

}

func paginate(page, pageSize int) (int, int) {
	if page <= 0 {
		page = 1
	}
	switch {
	case pageSize > 100:
		pageSize = 100
	case pageSize <= 0:
		pageSize = 10
	}
	offset := (page - 1) * pageSize
	return offset, pageSize
}
