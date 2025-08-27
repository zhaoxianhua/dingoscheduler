package dto

type Repository struct {
	ID            int64    `json:"id"`
	OrgRepo       string   `json:"orgRepo"`
	LikeNum       int      `json:"likeNum"`
	DownloadNum   int      `json:"downloadNum"`
	PipelineTagId string   `json:"pipelineTagId"`
	LastModified  string   `json:"lastModified"`
	Sha           string   `json:"sha"`
	Tags          []string `json:"tags,omitempty" `
}

type FileDescribe struct {
	Name  string `json:"name"`
	Size  int64  `json:"size"`
	IsDir bool   `json:"isDir"`
}
