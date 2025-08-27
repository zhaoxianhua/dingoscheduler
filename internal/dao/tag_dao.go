package dao

import (
	"dingoscheduler/internal/data"
	"dingoscheduler/internal/model"

	"go.uber.org/zap"
)

type TagDao struct {
	baseData *data.BaseData
}

func NewTagDao(data *data.BaseData) *TagDao {
	return &TagDao{
		baseData: data,
	}
}

// ExistsByID 检查标签是否已存在（根据id主键）
func (d *TagDao) ExistsByID(tagID string) (bool, error) {
	var count int64
	// 统计id=tagID的记录数
	err := d.baseData.BizDB.Table("tag").Where("id = ?", tagID).Count(&count).Error
	if err != nil {
		zap.S().Errorf("查询标签[id=%s]存在性失败：%v", tagID, err)
		return false, err
	}
	return count > 0, nil //  count>0表示已存在
}

// Create 插入新标签到数据库
func (d *TagDao) Create(tag *model.Tag) error {
	// 使用GORM的Create方法插入数据
	if err := d.baseData.BizDB.Table("tag").Create(tag).Error; err != nil {
		zap.S().Errorf("插入标签[id=%s]到数据库失败：%v", tag.ID, err)
		return err
	}
	return nil
}

func (d *TagDao) CreateBatch(tags []*model.Tag) error {
	if len(tags) == 0 {
		zap.S().Warn("批量插入的标签切片为空，无需执行插入")
		return nil
	}

	batchSize := 50
	result := d.baseData.BizDB.Model(&model.Tag{}).CreateInBatches(tags, batchSize)

	if result.Error != nil {
		zap.S().Errorf("批量插入标签失败，总数量：%d，错误：%v", len(tags), result.Error)
		return result.Error
	}

	if int(result.RowsAffected) != len(tags) {
		zap.S().Warnf("批量插入标签数量不匹配，预期插入：%d，实际插入：%d", len(tags), result.RowsAffected)
	} else {
		zap.S().Infof("批量插入标签成功，共插入 %d 条记录", result.RowsAffected)
	}

	return nil
}

func (d *TagDao) GetTagByRepoId(repoId int64) ([]*model.Tag, error) {
	var tags []*model.Tag
	err := d.baseData.BizDB.Table("tag t1").
		Where(" t1.id in (SELECT x.tag_id FROM repository_tag x where x.repo_id = ?)", repoId).Find(&tags).Error
	return tags, err
}
