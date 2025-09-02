package dto

type Repository struct {
	ID           int64    `json:"id"`
	OrgRepo      string   `json:"orgRepo"`
	LikeNum      int      `json:"likeNum"`
	DownloadNum  int      `json:"downloadNum"`
	PipelineTag  string   `json:"pipelineTag"`
	LastModified string   `json:"lastModified"`
	Sha          string   `json:"sha"`
	Tags         []string `json:"tags,omitempty" `
}
