package dto

type Tag struct {
	ID      string `gorm:"column:id;primaryKey;" json:"id"`
	Label   string `gorm:"column:label;not null" json:"label"`
	Type    string `gorm:"column:type;not null" json:"type"`
	SubType string `gorm:"column:sub_type;not null" json:"sub_type"`
}
