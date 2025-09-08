package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"dingoscheduler/internal/dao"
	"dingoscheduler/internal/data"
	"dingoscheduler/internal/model"
	"dingoscheduler/pkg/config"
	"dingoscheduler/pkg/util"

	"github.com/bytedance/sonic"
	"go.uber.org/zap"
)

// FileInfo 扩展结构用于存储文件解析后的关键信息
type FileInfo struct {
	DataType      string
	Org           string
	Repo          string
	FileName      string // 包含JSON文件的文件夹名称
	Etag          string // 来自Lfs.Oid或Oid
	FileSize      int64  // 来自size字段
	OriginContent []byte // 解码后的content内容
}

var (
	configPath    string
	repoPathParam string
	instanceID    string // 用户输入的InstanceID
	apiBaseURL    string // API基础地址，用于获取offset
	minFileSize   int64  // 最小文件大小阈值，单位为MB
)

func init() {
	flag.StringVar(&configPath, "config", "./config/config.yaml", "配置文件路径")
	flag.StringVar(&repoPathParam, "repoPath", "/Users/shijie/yonyou/dingospeed/repos", "仓库路径")
	flag.StringVar(&instanceID, "instanceId", "mas", "实例ID（必填）")
	flag.StringVar(&apiBaseURL, "apiBase", "http://127.0.0.1:8091", "获取offset的API基础地址")
	flag.Int64Var(&minFileSize, "minSize", 0, "最小文件大小阈值，单位为MB，小于此值的文件不录入数据库")
	flag.Parse()

	if instanceID == "" {
		zap.S().Fatal("必须提供instanceId参数，请使用 -instanceId 选项")
	}

	minFileSize *= 1024 * 1024
	zap.S().Infof("文件大小过滤阈值设置为: %d MB (%d 字节)", minFileSize/(1024*1024), minFileSize)
}

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Printf("初始化zap日志失败: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()
	zap.ReplaceGlobals(logger)

	fileInfos, err := processDirectory(repoPathParam)
	if err != nil {
		zap.S().Fatalf("处理目录失败: %v", err)
	}

	if len(fileInfos) == 0 {
		zap.S().Warn("未找到任何符合条件的文件进行处理")
		return
	}
	zap.S().Infof("本次从路径 %s 读取到 %d 个符合条件的文件", repoPathParam, len(fileInfos))

	filteredFileInfos := make([]FileInfo, 0)
	for _, fileInfo := range fileInfos {
		if fileInfo.FileSize >= minFileSize {
			filteredFileInfos = append(filteredFileInfos, fileInfo)
		} else {
			zap.S().Debugf("文件 %s 大小为 %d 字节，小于阈值 %d 字节，将被过滤",
				fileInfo.FileName, fileInfo.FileSize, minFileSize)
		}
	}

	zap.S().Infof("过滤后，本次需处理的文件剩余 %d 个（大于等于 %d MB）",
		len(filteredFileInfos), minFileSize/(1024*1024))

	if len(filteredFileInfos) == 0 {
		zap.S().Warn("所有文件都小于指定的大小阈值，没有文件将被处理")
		return
	}

	allCandidateRecords := make([]model.ModelFileRecord, 0, len(filteredFileInfos))
	for _, item := range filteredFileInfos {
		m := model.ModelFileRecord{
			Datatype: item.DataType,
			Org:      item.Org,
			Repo:     item.Repo,
			Name:     item.FileName,
			Etag:     item.Etag,
			FileSize: item.FileSize,
		}
		allCandidateRecords = append(allCandidateRecords, m)
	}

	conf, err := config.Scan(configPath)
	if err != nil {
		zap.S().Fatalf("读取配置文件失败: %v", err)
	}
	baseData, _, err := data.NewBaseData(conf)
	if err != nil {
		zap.S().Fatalf("初始化基础数据失败: %v", err)
	}

	modelFileRecordDao := dao.NewModelFileRecordDao(baseData)

	allCandidateEtags := make([]string, 0, len(allCandidateRecords))
	for _, r := range allCandidateRecords {
		allCandidateEtags = append(allCandidateEtags, r.Etag)
	}

	existingEtags, err := modelFileRecordDao.ExistEtags(allCandidateEtags)
	if err != nil {
		zap.S().Fatalf("查询已存在的Etag失败: %v", err)
	}
	existingEtagMap := make(map[string]struct{}, len(existingEtags))
	for _, etag := range existingEtags {
		existingEtagMap[etag] = struct{}{}
	}

	newRecords := make([]model.ModelFileRecord, 0)
	existingCandidateEtags := make([]string, 0)
	for _, r := range allCandidateRecords {
		if _, exists := existingEtagMap[r.Etag]; !exists {
			newRecords = append(newRecords, r)
		} else {
			existingCandidateEtags = append(existingCandidateEtags, r.Etag)
			zap.S().Infof("Etag %s 已存在于数据库（本次从路径读取到），无需重复添加到ModelFileRecord", r.Etag)
		}
	}

	if len(newRecords) > 0 {
		if err := modelFileRecordDao.BatchSave(newRecords); err != nil {
			zap.S().Fatalf("批量保存ModelFileRecord失败: %v", err)
		}
		zap.S().Infof("成功向ModelFileRecord添加 %d 条新记录（本次读取到的新记录）", len(newRecords))
	} else {
		zap.S().Info("本次读取到的记录均已存在于ModelFileRecord，无需新增")
	}

	newRecordIDs := make([]int64, 0, len(newRecords))
	for _, r := range newRecords {
		if r.ID != 0 {
			newRecordIDs = append(newRecordIDs, r.ID)
		} else {
			zap.S().Warnf("新记录Etag %s 保存后ID为0，跳过处理", r.Etag)
		}
	}

	existingRecordIDs := make([]int64, 0)
	if len(existingCandidateEtags) > 0 {
		existingRecordIDs, err = modelFileRecordDao.GetIDsByEtags(existingCandidateEtags)
		if err != nil {
			zap.S().Fatalf("根据Etag查询已存在记录的ID失败: %v", err)
		}
		zap.S().Infof("本次读取到的已存在记录中，共查询到 %d 个有效ID", len(existingRecordIDs))
	}

	allNeedProcessRecordIDs := append(newRecordIDs, existingRecordIDs...)
	if len(allNeedProcessRecordIDs) == 0 {
		zap.S().Info("本次读取到的记录中，没有有效RecordID，无需生成ModelFileProcess记录")
		return
	}
	zap.S().Infof("本次需为 %d 个RecordID生成ModelFileProcess记录（新记录: %d, 已存在记录: %d）",
		len(allNeedProcessRecordIDs), len(newRecordIDs), len(existingRecordIDs))

	modelFileProcessDao := dao.NewModelFileProcessDao(baseData)
	recordInfoMap := make(map[int64]model.ModelFileRecord)
	for _, r := range newRecords {
		if r.ID != 0 {
			recordInfoMap[r.ID] = r
		}
	}

	if len(existingRecordIDs) > 0 {
		existingRecords, err := modelFileRecordDao.GetByIDs(existingRecordIDs)
		if err != nil {
			zap.S().Fatalf("根据ID查询已存在的ModelFileRecord记录失败: %v", err)
		}
		for _, r := range existingRecords {
			recordInfoMap[r.ID] = r
		}
	}

	processRecords := make([]model.ModelFileProcess, 0, len(allNeedProcessRecordIDs))
	for _, recordID := range allNeedProcessRecordIDs {
		record, exists := recordInfoMap[recordID]
		if !exists {
			zap.S().Warnf("RecordID %d 未找到对应的完整信息，跳过生成Process记录", recordID)
			continue
		}

		offset, err := getOffsetValue(record.Datatype, record.Org, record.Repo, record.Etag, record.FileSize)
		if err != nil {
			zap.S().Warnf("RecordID %d 获取offset失败: %v，使用默认值0", recordID, err)
			offset = 0
		}

		processRecords = append(processRecords, model.ModelFileProcess{
			RecordID:   recordID,
			InstanceID: instanceID,
			OffsetNum:  offset,
			Status:     3,
		})
	}

	existingProcessRecordIDs, err := modelFileProcessDao.ExistRecordIDs(instanceID, allNeedProcessRecordIDs)
	if err != nil {
		zap.S().Fatalf("查询已存在的ModelFileProcess记录失败: %v", err)
	}
	existingProcessRecordIDMap := make(map[int64]struct{}, len(existingProcessRecordIDs))
	for _, id := range existingProcessRecordIDs {
		existingProcessRecordIDMap[id] = struct{}{}
	}

	newProcessRecords := make([]model.ModelFileProcess, 0, len(processRecords))
	for _, p := range processRecords {
		if _, exists := existingProcessRecordIDMap[p.RecordID]; !exists {
			newProcessRecords = append(newProcessRecords, p)
		} else {
			zap.S().Infof("RecordID %d 在InstanceID %s 下已存在ModelFileProcess记录，本次跳过", p.RecordID, p.InstanceID)
		}
	}

	if len(newProcessRecords) > 0 {
		if err := modelFileProcessDao.BatchSave(newProcessRecords); err != nil {
			zap.S().Fatalf("批量保存ModelFileProcess失败: %v", err)
		}
		zap.S().Infof("成功向ModelFileProcess添加 %d 条新记录（基于本次读取到的文件）", len(newProcessRecords))
	} else {
		zap.S().Info("本次读取到的RecordID中，对应的ModelFileProcess记录均已存在，无需新增")
	}

	zap.S().Infof("程序执行完成，本次共处理 %d 个文件，最终新增 %d 条ModelFileProcess记录",
		len(filteredFileInfos), len(newProcessRecords))
}

// getOffsetValue 调用API获取offset值（实现不变）
func getOffsetValue(dataType, org, repo, etag string, fileSize int64) (int64, error) {
	url := fmt.Sprintf("%s/api/%s/%s/%s/%s/%d",
		apiBaseURL, dataType, org, repo, etag, fileSize)

	resp, err := http.Get(url)
	if err != nil {
		return 0, fmt.Errorf("请求API失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("API返回非成功状态码: %d, URL: %s", resp.StatusCode, url)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("读取响应内容失败: %w", err)
	}

	var offset int64
	if err := json.Unmarshal(body, &offset); err != nil {
		return 0, fmt.Errorf("解析offset值失败: %w, 响应内容: %s, URL: %s", err, string(body), url)
	}

	return offset, nil
}

func processDirectory(rootPath string) ([]FileInfo, error) {
	var result []FileInfo

	if exists, err := fileExists(rootPath); err != nil {
		zap.S().Errorf("检查根目录存在性失败: %v", err)
		return nil, err
	} else if !exists {
		zap.S().Errorf("根目录不存在: %s", rootPath)
		return nil, os.ErrNotExist
	}

	err := filepath.WalkDir(rootPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			zap.S().Warnf("访问路径时出错 %s: %v", path, err)
			return nil
		}

		if path == rootPath {
			return nil
		}

		relPath, err := filepath.Rel(rootPath, path)
		if err != nil {
			zap.S().Warnf("获取相对路径失败 %s: %v", path, err)
			return nil
		}

		components := strings.Split(relPath, string(filepath.Separator))
		if len(components) < 5 ||
			components[0] != "api" ||
			!(components[1] == "models" || components[1] == "datasets" || components[1] == "spaces") ||
			components[4] != "paths-info" {
			return nil
		}

		if d.IsDir() {
			jsonFilePath := filepath.Join(path, "paths-info_post.json")
			if exists, err := fileExists(jsonFilePath); err != nil {
				zap.S().Warnf("检查文件存在性失败 %s: %v", jsonFilePath, err)
				return nil
			} else if exists {
				zap.S().Debugf("找到符合条件的文件: %s", jsonFilePath)
				var pathSegments []string
				if len(components) >= 7 {
					pathSegments = components[6:]
				} else {
					pathSegments = []string{}
				}

				fullPath := filepath.Join(pathSegments...)
				fileInfo, err := processJsonFile(
					jsonFilePath,
					components[1],
					components[2],
					components[3],
					fullPath,
				)
				if err != nil {
					zap.S().Warnf("处理JSON文件失败 %s: %v", jsonFilePath, err)
					return nil
				}

				if fileInfo != nil {
					result = append(result, *fileInfo)
				}
			}
		}

		return nil
	})

	return result, err
}

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func processJsonFile(jsonPath, dataType, org, repo, fileName string) (*FileInfo, error) {
	bytes, err := util.ReadFileToBytes(jsonPath)
	if err != nil {
		return nil, err
	}

	var cacheContent CacheContent
	if err = sonic.Unmarshal(bytes, &cacheContent); err != nil {
		return nil, err
	}

	decodeByte, err := hex.DecodeString(cacheContent.Content)
	if err != nil {
		return nil, err
	}

	var contentItems []ContentItem
	if err = sonic.Unmarshal(decodeByte, &contentItems); err != nil {
		return nil, err
	}

	if len(contentItems) == 0 {
		zap.S().Warnf("JSON文件 %s 中的content数组为空", jsonPath)
		return nil, nil
	}

	item := contentItems[0]
	etag := item.Oid
	if item.Lfs != nil && item.Lfs.Oid != "" {
		etag = item.Lfs.Oid
	}

	return &FileInfo{
		DataType:      dataType,
		Org:           org,
		Repo:          repo,
		FileName:      fileName,
		Etag:          etag,
		FileSize:      item.Size,
		OriginContent: decodeByte,
	}, nil
}

type CacheContent struct {
	Content string `json:"content"`
}
type ContentItem struct {
	Type string   `json:"type"`
	Oid  string   `json:"oid"`
	Size int64    `json:"size"`
	Lfs  *LfsInfo `json:"lfs"`
	Path string   `json:"path"`
}
type LfsInfo struct {
	Oid  string `json:"oid"`
	Size int64  `json:"size"`
}
