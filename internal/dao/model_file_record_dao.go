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
	"dingoscheduler/internal/model/dto"
	"dingoscheduler/internal/model/query"
)

type ModelFileRecordDao struct {
	baseData *data.BaseData
}

func NewModelFileRecordDao(data *data.BaseData) *ModelFileRecordDao {
	return &ModelFileRecordDao{
		baseData: data,
	}
}

func (d *ModelFileRecordDao) BatchSave(records []model.ModelFileRecord) error {
	if err := d.baseData.BizDB.Model(&model.ModelFileRecord{}).CreateInBatches(&records, 5).Error; err != nil {
		return err
	}
	return nil
}

func (d *ModelFileRecordDao) FindModelFileRecords(condition *query.ModelFileRecordQuery) ([]*dto.ModelFileRecordDto, error) {
	records := make([]*dto.ModelFileRecordDto, 0)
	db := d.baseData.BizDB.Model(&model.ModelFileRecord{}).Select("model_file_record.*, dingospeed.host, dingospeed.port").Joins("left join dingospeed on model_file_record.area_instance = dingospeed.area_instance")
	if condition.Datatype != "" {
		db.Where("datatype = ?", condition.Datatype)
	}
	if condition.Org != "" {
		db.Where("org = ?", condition.Org)
	}
	if condition.Repo != "" {
		db.Where("repo = ?", condition.Repo)
	}
	if condition.Etag != "" {
		db.Where("etag = ?", condition.Etag)
	}
	if err := db.Order("complete_at desc").Find(&records).Error; err != nil {
		return nil, err
	}
	return records, nil
}
