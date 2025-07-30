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
	"errors"
	"fmt"

	"dingoscheduler/internal/data"
	"dingoscheduler/internal/model"
	"dingoscheduler/internal/model/dto"
	"dingoscheduler/pkg/util"

	"gorm.io/gorm"
)

type ModelFileProcessDao struct {
	baseData *data.BaseData
}

func NewModelFileProcessDao(data *data.BaseData) *ModelFileProcessDao {
	return &ModelFileProcessDao{
		baseData: data,
	}
}

func (d *ModelFileProcessDao) Save(process *model.ModelFileProcess) error {
	if err := d.baseData.BizDB.Model(&model.ModelFileProcess{}).Save(process).Error; err != nil {
		return err
	}
	return nil
}

func (d *ModelFileProcessDao) Update(process *model.ModelFileProcess, startPos int64) error {
	var offsetSql string
	if startPos != 0 {
		offsetSql = fmt.Sprintf(" and offset_num >= %d", startPos)
	}
	sql := fmt.Sprintf("UPDATE model_file_process SET offset_num = %d, status = %d, updated_at = '%s' WHERE id = %d %s",
		process.OffsetNum, process.Status, util.GetCurrentTimeStr(), process.ID, offsetSql)
	if err := d.baseData.BizDB.Exec(sql).Error; err != nil {
		return err
	}
	return nil
}

func (d *ModelFileProcessDao) GetModelFileProcess(recordId int64) ([]*dto.ModelFileProcessDto, error) {
	var processes []*dto.ModelFileProcessDto
	if err := d.baseData.BizDB.Table("model_file_process t1").Select("t1.id, t1.record_id, t1.instance_id, t1.offset_num, t2.host, t2.port, t2.updated_at").
		Joins("left join dingospeed t2 on t1.instance_id = t2.instance_id and t2.online = true").
		Where("record_id=?", recordId).Order("t1.offset_num desc").Find(&processes).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return processes, nil
}
