package model

import (
	"time"
)

const TableNameRepository = "repository"

// PreheatJob mapped from table <preheat_job>
type Repository struct {
	ID           int64     `gorm:"column:id;primaryKey;autoIncrement:true" json:"id"`
	Area         string    `gorm:"column:area;not null" json:"area"`
	Datatype     string    `gorm:"column:datatype;not null" json:"datatype"`
	Org          string    `gorm:"column:org;not null" json:"org"`
	Repo         string    `gorm:"column:repo;not null" json:"repo"`
	OrgRepo      string    `gorm:"column:org_repo;not null" json:"org_repo"`
	LikeNum      int       `gorm:"column:like_num;not null" json:"like_num"`
	FollowNum    int       `gorm:"column:follow_num;not null" json:"follow_num"`
	PiplineTagId int       `gorm:"column:pipline_tag_id;not null" json:"pipline_tag_id"`
	LastModified string    `gorm:"column:last_modified;not null" json:"last_modified"`
	UsedStorage  int64     `gorm:"column:used_storage;" json:"used_storage"`
	CreatedAt    time.Time `gorm:"column:created_at;not null;default:CURRENT_TIMESTAMP" json:"created_at"`
	UpdatedAt    time.Time `gorm:"column:updated_at;not null;default:CURRENT_TIMESTAMP" json:"updated_at"`
}

// TableName PreheatJob's table name
func (*Repository) TableName() string {
	return TableNameRepository
}
