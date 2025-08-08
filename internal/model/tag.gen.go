package model

const TableNameTag = "tag"

type Tag struct {
	ID    string `gorm:"column:id;primaryKey;" json:"id"`
	Label string `gorm:"column:label;not null" json:"label"`
	Type  string `gorm:"column:type;not null" json:"type"`
}

// TableName Organization's table name
func (*Tag) TableName() string {
	return TableNameTag
}
