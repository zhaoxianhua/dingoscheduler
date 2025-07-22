package query

type ModelFileRecordQuery struct {
	InstanceId string
	Datatype   string
	Org        string
	Repo       string
	Etag       string
	StartPos   int64
}
