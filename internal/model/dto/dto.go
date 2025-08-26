package dto

type Repository struct {
	ID            int64  `json:"id"`
	OrgRepo       string `json:"orgRepo"`
	LikeNum       int    `json:"likeNum"`
	DownloadNum   int    `json:"downloadNum"`
	PipelineTagId string `json:"pipelineTagId"`
	LastModified  string `json:"lastModified"`
}
