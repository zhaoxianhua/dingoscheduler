package main

import (
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"dingoscheduler/internal/dao"
	"dingoscheduler/internal/data"
	"dingoscheduler/internal/model"
	"dingoscheduler/pkg/config"
	"dingoscheduler/pkg/util"

	"github.com/bytedance/sonic"
	"go.uber.org/zap"
)

type FileInfo struct {
	DataType      string
	Org           string
	Repo          string
	FileName      string
	Etag          string
	FileSize      int64
	OriginContent []byte
}

var (
	configPath    string
	repoPathParam string
	instanceID    string
	apiBaseURL    string
	minFileSize   int64
)

func init() {
	flag.StringVar(&configPath, "config", "./config/config.yaml", "配置文件路径")
	flag.StringVar(&repoPathParam, "repoPath", "/Users/shijie/yonyou/dingospeed/repos", "仓库路径")
	flag.StringVar(&instanceID, "instanceId", "mas", "实例ID（必填）")
	flag.StringVar(&apiBaseURL, "apiBase", "http://127.0.0.:8091", "获取offset的API基础地址")
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

	etagSet := make(map[string]struct{})
	for _, item := range filteredFileInfos {
		if item.Etag != "" {
			etagSet[item.Etag] = struct{}{}
		} else {
			zap.S().Debugf("文件 %s 的etag为空，将被跳过", item.FileName)
		}
	}

	etags := make([]string, 0, len(etagSet))
	for etag := range etagSet {
		etags = append(etags, etag)
	}
	zap.S().Infof("共提取到 %d 个唯一的etag用于查询", len(etags))

	conf, err := config.Scan(configPath)
	if err != nil {
		zap.S().Fatalf("读取配置文件失败: %v", err)
	}
	baseData, _, err := data.NewBaseData(conf)
	if err != nil {
		zap.S().Fatalf("初始化基础数据失败: %v", err)
	}

	modelFileRecordDao := dao.NewModelFileRecordDao(baseData)
	batchSize := 1000
	totalCount := len(etags)
	allNeedProcessRecordIDs := make([]int64, 0)

	if totalCount == 0 {
		zap.S().Warn("没有有效的etag用于查询，程序将退出")
		return
	}

	totalBatches := (totalCount + batchSize - 1) / batchSize
	zap.S().Infof("将分 %d 批查询ModelFileRecord ID，每批最多 %d 个etag", totalBatches, batchSize)
	for i := 0; i < totalBatches; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > totalCount {
			end = totalCount
		}

		batchEtags := etags[start:end]
		zap.S().Debugf("正在查询第 %d/%d 批，处理 %d-%d 个etag", i+1, totalBatches, start, end-1)

		batchIDs, err := modelFileRecordDao.BatchQueryIDsByEtags(batchEtags)
		if err != nil {
			zap.S().Fatalf("第 %d 批查询ModelFileRecord ID失败: %v", i+1, err)
		}

		allNeedProcessRecordIDs = append(allNeedProcessRecordIDs, batchIDs...)
	}

	zap.S().Infof("所有批次查询完成，共获取 %d 个ModelFileRecord ID", len(allNeedProcessRecordIDs))
	idSet := make(map[int64]struct{}, len(allNeedProcessRecordIDs))
	for _, id := range allNeedProcessRecordIDs {
		idSet[id] = struct{}{}
	}
	allNeedProcessRecordIDs = make([]int64, 0, len(idSet))
	for id := range idSet {
		allNeedProcessRecordIDs = append(allNeedProcessRecordIDs, id)
	}

	if len(allNeedProcessRecordIDs) == 0 {
		zap.S().Info("本次读取到的记录中，没有匹配的ModelFileRecord ID，无需生成ModelFileProcess记录")
		return
	}
	zap.S().Infof("本次需为 %d 个RecordID生成ModelFileProcess记录", len(allNeedProcessRecordIDs))

	records, err := modelFileRecordDao.BatchQueryByIDs(allNeedProcessRecordIDs)
	if err != nil {
		zap.S().Fatalf("批量查询ModelFileRecord详情失败: %v", err)
	}
	recordInfoMap := make(map[int64]model.ModelFileRecord, len(records))
	for _, record := range records {
		recordInfoMap[record.ID] = record
	}

	for _, id := range allNeedProcessRecordIDs {
		if _, exists := recordInfoMap[id]; !exists {
			zap.S().Warnf("RecordID %d 未找到对应的完整信息，将被跳过", id)
		}
	}

	modelFileProcessDao := dao.NewModelFileProcessDao(baseData)
	processRecords := make([]model.ModelFileProcess, 0, len(allNeedProcessRecordIDs))
	for _, recordID := range allNeedProcessRecordIDs {
		record, exists := recordInfoMap[recordID]
		if !exists {
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
		processFile, err := os.OpenFile(
			"model_file_process.csv",
			os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
			0644,
		)
		if err != nil {
			zap.S().Fatalf("创建ModelFileProcess CSV文件失败: %v", err)
		}
		defer func() {
			processFile.Close()
			zap.S().Debugf("ModelFileProcess CSV文件句柄已关闭")
		}()

		processWriter := csv.NewWriter(processFile)
		processWriter.Comma = ','
		processWriter.UseCRLF = true
		defer func() {
			processWriter.Flush()
			if err := processWriter.Error(); err != nil {
				zap.S().Errorf("刷新ModelFileProcess CSV缓存失败: %v", err)
			}
		}()

		processHeader := []string{"record_id", "instance_id", "offset_num", "status"}
		if err := processWriter.Write(processHeader); err != nil {
			zap.S().Fatalf("写入ModelFileProcess CSV表头失败: %v", err)
		}

		for _, process := range newProcessRecords {
			dataRow := []string{
				strconv.FormatInt(process.RecordID, 10),
				process.InstanceID,
				strconv.FormatInt(process.OffsetNum, 10),
				strconv.Itoa(int(process.Status)),
			}
			if err := processWriter.Write(dataRow); err != nil {
				zap.S().Fatalf("写入ModelFileProcess数据失败（RecordID: %d）: %v", process.RecordID, err)
			}
		}

		zap.S().Infof("成功生成ModelFileProcess CSV文件，包含 %d 条新记录: model_file_process.csv", len(newProcessRecords))
		zap.S().Infof("=== MySQL导入命令参考 ===")
		zap.S().Infof("LOAD DATA INFILE '/path/to/model_file_process.csv'")
		zap.S().Infof("INTO TABLE model_file_process")
		zap.S().Infof("FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\\\\'")
		zap.S().Infof("LINES TERMINATED BY '\\r\\n'")
		zap.S().Infof("IGNORE 1 ROWS (record_id, instance_id, offset_num, status);")
		zap.S().Infof("==========================")
	} else {
		zap.S().Info("本次读取到的RecordID中，对应的ModelFileProcess记录均已存在，无需生成CSV文件")
	}

	zap.S().Infof("程序执行完成，本次共处理 %d 个文件，最终生成 %d 条ModelFileProcess CSV记录",
		len(filteredFileInfos), len(newProcessRecords))
}

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
