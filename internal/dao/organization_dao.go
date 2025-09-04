package dao

import (
	"dingoscheduler/internal/data"
	"dingoscheduler/internal/model"
	"go.uber.org/zap"
)

type OrganizationDao struct {
	baseData *data.BaseData
}

func NewOrganizationDao(data *data.BaseData) *OrganizationDao {
	return &OrganizationDao{
		baseData: data,
	}
}

// organizationDao的相关方法示例
func (d *OrganizationDao) ExistsByField(field, value string) (bool, error) {
	var count int64
	err := d.baseData.BizDB.Table("organization").Where(field+" = ?", value).Count(&count).Error
	return count > 0, err
}

func (d *OrganizationDao) Insert(org *model.Organization) error {
	return d.baseData.BizDB.Table("organization").Create(org).Error
}

func (d *OrganizationDao) UpdateByField(field, value string, org *model.Organization) error {
	return d.baseData.BizDB.Table("organization").Where(field+" = ?", value).Updates(org).Error
}

func (d *OrganizationDao) FindAllNames() ([]string, error) {
	var names []string
	err := d.baseData.BizDB.Table("organization").Pluck("name", &names).Error
	if err != nil {
		zap.S().Errorf("查询organization表name字段失败：%v", err)
		return nil, err
	}
	return names, nil
}
