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
	pb "dingoscheduler/pkg/proto/manager"
	"errors"
	"go.uber.org/zap"

	"dingoscheduler/internal/data"
	"dingoscheduler/internal/model"
	"dingoscheduler/internal/model/query"

	"gorm.io/gorm"
)

type ModelFileRecordDao struct {
	baseData *data.BaseData
}

func NewModelFileRecordDao(data *data.BaseData) *ModelFileRecordDao {
	return &ModelFileRecordDao{
		baseData: data,
	}
}

func (d *ModelFileRecordDao) Save(records *model.ModelFileRecord) error {
	if err := d.baseData.BizDB.Model(&model.ModelFileRecord{}).Save(records).Error; err != nil {
		return err
	}
	return nil
}

func (d *ModelFileRecordDao) BatchSave(records []model.ModelFileRecord) error {
	if err := d.baseData.BizDB.Model(&model.ModelFileRecord{}).CreateInBatches(&records, 5).Error; err != nil {
		return err
	}
	return nil
}

func (d *ModelFileRecordDao) GetModelFileRecord(condition *query.ModelFileRecordQuery) (*model.ModelFileRecord, error) {
	record := model.ModelFileRecord{}
	db := d.baseData.BizDB.Model(&model.ModelFileRecord{}).Select("id")
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
	if err := db.First(&record).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &record, nil
}

func (d *ModelFileRecordDao) SaveSchedulerRecord(req *pb.SchedulerFileRequest, process *model.ModelFileProcess) error {
	if err := d.baseData.BizDB.Transaction(func(tx *gorm.DB) error {
		record := &model.ModelFileRecord{
			Datatype: req.DataType,
			Org:      req.Org,
			Repo:     req.Repo,
			Name:     req.Name,
			Etag:     req.Etag,
			FileSize: req.FileSize,
		}
		if err := tx.Create(record).Error; err != nil {
			return err
		}
		process.RecordID = record.ID
		process.Offset = 0 // 初始
		if err := tx.Create(process).Error; err != nil {
			return err
		}
		return nil
	}); err != nil {
		zap.S().Error("SaveSchedulerRecord err.%v", err)
		return err
	}
	return nil
}
