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
	"dingoscheduler/internal/data"
	"dingoscheduler/internal/model"
)

type RepositoryDao struct {
	baseData *data.BaseData
}

func NewRepositoryDao(data *data.BaseData) *RepositoryDao {
	return &RepositoryDao{
		baseData: data,
	}
}

func (d *RepositoryDao) Save(repository *model.Repository) error {
	if err := d.baseData.BizDB.Model(&model.Repository{}).Save(repository).Error; err != nil {
		return err
	}
	return nil
}

func (d *RepositoryDao) GetFreeRepository() ([]*model.Repository, error) {
	var repositories []*model.Repository
	err := d.baseData.BizDB.Table("model_file_record t1").Select("distinct t1.datatype, t1.org, t1.repo ").
		Where("repo not in (select repo from repository)").Find(&repositories).Error
	return repositories, err
}
