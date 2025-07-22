package dto

import "time"

type ModelFileProcessDto struct {
	ID               int64     `gorm:"column:id;primaryKey;autoIncrement:true" json:"id"`
	RecordID         int64     `gorm:"column:record_id;not null" json:"record_id"`
	InstanceID       string    `gorm:"column:instance_id;not null" json:"instance_id"`
	Offset           int64     `gorm:"column:offset;not null" json:"offset"`
	Status           int32     `gorm:"column:status;not null;comment:下载状态：1(正在下载)，2（下载中断），3（下载完成）" json:"status"` // 下载状态：1(正在下载)，2（下载中断），3（下载完成）
	MasterInstanceID string    `gorm:"column:master_instance_id" json:"master_instance_id"`
	CreatedAt        time.Time `gorm:"column:created_at;not null;default:CURRENT_TIMESTAMP" json:"created_at"`
	UpdatedAt        time.Time `gorm:"column:updated_at" json:"updated_at"`
	Host             string    `gorm:"column:host;not null" json:"host"`
	Port             int32     `gorm:"column:port;not null" json:"port"`
}
