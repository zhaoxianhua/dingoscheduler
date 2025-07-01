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

	"dingoscheduler/internal/data"
	"dingoscheduler/internal/model"

	"gorm.io/gorm"
)

type DingospeedDao struct {
	baseData *data.BaseData
}

func NewDingospeedDao(data *data.BaseData) *DingospeedDao {
	return &DingospeedDao{
		baseData: data,
	}
}

func (d *DingospeedDao) Save(Config model.Dingospeed) error {
	if err := d.baseData.BizDB.Model(&model.Dingospeed{}).Save(&Config).Error; err != nil {
		return err
	}
	return nil
}

func (d *DingospeedDao) Update(speed model.Dingospeed) error {
	if err := d.baseData.BizDB.Model(&model.Dingospeed{}).Where("id=?", speed.ID).Updates(&speed).Error; err != nil {
		return err
	}
	return nil
}

func (d *DingospeedDao) UpdateForMap(id int32, values map[string]interface{}) error {
	if err := d.baseData.BizDB.Model(&model.Dingospeed{}).Where("id=?", id).Updates(values).Error; err != nil {
		return err
	}
	return nil
}

func (d *DingospeedDao) GetEntityById(id int32) (*model.Dingospeed, error) {
	var speed model.Dingospeed
	if err := d.baseData.BizDB.Model(&model.Dingospeed{}).Where("id = ?", id).Find(&speed).Error; err != nil {
		return nil, err
	}
	return &speed, nil
}

func (d *DingospeedDao) GetEntity(area, host string, port int32) (*model.Dingospeed, error) {
	var speed model.Dingospeed
	if err := d.baseData.BizDB.Model(&model.Dingospeed{}).Where("area = ? and host=? and port=?", area, host, port).First(&speed).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &speed, nil
}
