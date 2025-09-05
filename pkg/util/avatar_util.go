package util

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"go.uber.org/zap"
	"golang.org/x/net/html"
)

const baseURL = "https://huggingface.co" // Hugging Face 基础URL

func FetchAvatarURL(orgName string) (string, error) {
	orgURL := fmt.Sprintf("%s/%s", baseURL, orgName)
	zap.S().Debugf("开始请求Hugging Face组织页面：%s", orgURL) // 调试级日志，记录请求地址

	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // 跳过TLS验证（视环境调整）
		},
	}

	req, err := http.NewRequest("GET", orgURL, nil)
	if err != nil {
		return "", fmt.Errorf("创建HTTP请求失败：%w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")
	zap.S().Debugf("已构建请求，User-Agent：%s", req.Header.Get("User-Agent"))

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("请求组织页面（%s）失败：%w", orgURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		zap.S().Errorf("组织页面（%s）响应异常，状态码：%d", orgURL, resp.StatusCode)
		return "", fmt.Errorf("组织页面状态异常（%s）：状态码 %d", orgURL, resp.StatusCode)
	}
	zap.S().Debugf("成功获取组织页面（%s），状态码：200", orgURL)

	doc, err := html.Parse(resp.Body)
	if err != nil {
		return "", fmt.Errorf("解析HTML页面（%s）失败：%w", orgURL, err)
	}

	var avatarURL string
	var findAvatar func(*html.Node)
	findAvatar = func(n *html.Node) {
		if avatarURL != "" {
			return // 已找到，提前退出
		}

		if n.Type == html.ElementNode && n.Data == "div" {
			for _, attr := range n.Attr {
				if attr.Key == "class" &&
					strings.Contains(attr.Val, "relative") &&
					strings.Contains(attr.Val, "mr-4") &&
					strings.Contains(attr.Val, "flex") &&
					(strings.Contains(attr.Val, "size-16") || strings.Contains(attr.Val, "size-20")) {

					zap.S().Debugf("找到疑似头像外层div，class：%s", attr.Val)

					for c := n.FirstChild; c != nil; c = c.NextSibling {
						if c.Type == html.ElementNode && c.Data == "a" {
							isTargetOrgLink := false
							var matchedAttr string
							for _, aAttr := range c.Attr {
								if (aAttr.Key == "href" && strings.Trim(aAttr.Val, "/") == orgName) ||
									(aAttr.Key == "title" && strings.Contains(strings.ToLower(aAttr.Val), strings.ToLower(orgName))) {
									isTargetOrgLink = true
									matchedAttr = fmt.Sprintf("%s=%s", aAttr.Key, aAttr.Val)
									break
								}
							}
							if !isTargetOrgLink {
								continue
							}
							zap.S().Debugf("找到目标a标签（%s），开始查找内部img", matchedAttr)

							for imgNode := c.FirstChild; imgNode != nil; imgNode = imgNode.NextSibling {
								if imgNode.Type == html.ElementNode && imgNode.Data == "img" {
									for _, imgAttr := range imgNode.Attr {
										if imgAttr.Key == "src" && imgAttr.Val != "" {
											if strings.Contains(imgAttr.Val, "cdn-avatars.huggingface.co") ||
												strings.Contains(imgAttr.Val, "/avatars/") ||
												strings.Contains(imgAttr.Val, orgName) {
												avatarURL = imgAttr.Val
												zap.S().Debugf("成功匹配头像img，src：%s", avatarURL)
												return
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}

		for c := n.FirstChild; c != nil; c = c.NextSibling {
			findAvatar(c)
			if avatarURL != "" {
				return
			}
		}
	}
	findAvatar(doc)

	if avatarURL == "" {
		zap.S().Errorf("在组织页面（%s）中未找到符合特征的头像节点", orgURL)
		return "", fmt.Errorf("未在组织页面（%s）中找到头像元素", orgURL)
	}

	if !strings.HasPrefix(avatarURL, "http") {
		zap.S().Debugf("头像URL为相对路径，需补全：%s", avatarURL)
		switch {
		case strings.HasPrefix(avatarURL, "//"):
			avatarURL = "https:" + avatarURL
		case strings.HasPrefix(avatarURL, "/"):
			base, _ := url.Parse(baseURL)
			parsed, _ := url.Parse(avatarURL)
			avatarURL = base.ResolveReference(parsed).String()
		default:
			parsedOrgURL, _ := url.Parse(orgURL)
			parsedSrc, _ := url.Parse(avatarURL)
			avatarURL = parsedOrgURL.ResolveReference(parsedSrc).String()
		}
		zap.S().Debugf("相对路径补全后，完整头像URL：%s", avatarURL)
	}

	return avatarURL, nil
}

func DownloadAvatar(
	avatarURL, saveRoot, subDir, org, ossBucketName string,
	ossOption *ImageUploadOption, // 引用UploadImageToOSS的ImageUploadOption类型
) (string, error) {
	localFileName := fmt.Sprintf("%s_%s_avatar.jpg", subDir, org)
	localSavePath := filepath.Join(saveRoot, localFileName)
	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	resp, err := client.Get(avatarURL)
	if err != nil {
		return "", fmt.Errorf("下载请求失败（URL=%s）：%w", avatarURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		zap.S().Errorf("头像下载响应异常：URL=%s，状态码=%d", avatarURL, resp.StatusCode)
		return "", fmt.Errorf("下载状态异常（URL=%s，状态码=%d）", avatarURL, resp.StatusCode)
	}
	zap.S().Debugf("头像文件流获取成功：URL=%s，预估大小=%d bytes", avatarURL, resp.ContentLength)

	localFile, err := os.Create(localSavePath)
	if err != nil {
		return "", fmt.Errorf("创建本地暂存文件（路径=%s）失败：%w", localSavePath, err)
	}
	defer localFile.Close()

	if _, err := io.Copy(localFile, resp.Body); err != nil {
		return "", fmt.Errorf("写入本地暂存文件（路径=%s）失败：%w", localSavePath, err)
	}

	ossObjectKey := fmt.Sprintf("assets/static/bp/model/%s", localFileName)
	uploadResult, err := UploadImageToOSS(
		ossBucketName,
		ossObjectKey,
		localSavePath,
		ossOption,
	)
	if err != nil {
		return "", fmt.Errorf("OSS上传失败（OSS路径=%s）：%w", ossObjectKey, err)
	}

	zap.S().Debugf("头像OSS上传成功：桶名=%s，OSS文件名称=%s，ETag=%s",
		ossBucketName, uploadResult.ObjectKey, uploadResult.ETag)

	if err := os.Remove(localSavePath); err != nil {
		zap.S().Warnf("删除本地暂存文件（路径=%s）失败：%v", localSavePath, err)
	}

	return uploadResult.ObjectKey, nil
}

type ImageUploadOption struct {
	Region        string
	StorageClass  oss.StorageClassType
	ObjectACL     oss.ObjectACLType
	Timeout       time.Duration
	CustomHeaders map[string]string
}

type ImageUploadResult struct {
	BucketName string
	ObjectKey  string
	ETag       string
	RequestId  string
	StatusCode int
	FileSize   int64
}

func isValidImageFormat(fileExt string) bool {
	validExts := []string{".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"}
	fileExt = strings.ToLower(fileExt)
	for _, ext := range validExts {
		if fileExt == ext {
			return true
		}
	}
	return false
}

func UploadImageToOSS(
	bucketName, objectKey, localImagePath string,
	option *ImageUploadOption,
) (*ImageUploadResult, error) {
	if bucketName == "" {
		return nil, errors.New("bucket名称不能为空")
	}
	if objectKey == "" {
		return nil, errors.New("OSS对象名（objectKey）不能为空")
	}
	if localImagePath == "" {
		return nil, errors.New("本地图片路径不能为空")
	}

	fileInfo, err := os.Stat(localImagePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.New("本地图片文件不存在：" + localImagePath)
		}
		return nil, errors.New("获取图片文件信息失败：" + err.Error())
	}
	if fileInfo.IsDir() {
		return nil, errors.New("本地路径是目录，不是图片文件：" + localImagePath)
	}
	fileExt := filepath.Ext(localImagePath)
	if !isValidImageFormat(fileExt) {
		return nil, errors.New("不支持的图片格式：" + fileExt + "，支持格式：jpg/jpeg/png/gif/bmp/webp")
	}

	if option == nil {
		option = &ImageUploadOption{
			Region:       "cn-beijing",             // 默认华北2（北京）地域
			StorageClass: oss.StorageClassStandard, // 标准存储（oss根包常量）
			ObjectACL:    oss.ObjectACLPrivate,     // 私有访问（oss根包常量）
			Timeout:      10 * time.Second,         // 默认10秒超时
		}
	} else {
		if option.Region == "" {
			option.Region = "cn-beijing"
		}
		if option.StorageClass == "" {
			option.StorageClass = oss.StorageClassStandard
		}
		if option.ObjectACL == "" {
			option.ObjectACL = oss.ObjectACLPrivate
		}
		if option.Timeout <= 0 {
			option.Timeout = 10 * time.Second
		}
	}

	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewEnvironmentVariableCredentialsProvider()).
		WithRegion(option.Region)

	client := oss.NewClient(cfg)
	file, err := os.Open(localImagePath)
	if err != nil {
		return nil, errors.New("打开图片文件失败：" + err.Error())
	}
	defer file.Close()

	ctx, cancel := context.WithTimeout(context.Background(), option.Timeout)
	defer cancel()

	putReq := &oss.PutObjectRequest{
		Bucket:        oss.Ptr(bucketName),
		Key:           oss.Ptr(objectKey),
		Body:          file,
		ContentLength: oss.Ptr(fileInfo.Size()),
	}
	result, err := client.PutObject(ctx, putReq)
	if err != nil {
		return nil, errors.New("OSS图片上传失败：" + err.Error())
	}

	return &ImageUploadResult{
		BucketName: bucketName,
		ObjectKey:  *putReq.Key,
		ETag:       *result.ETag,
		RequestId:  result.ResultCommon.Headers.Get("X-Oss-Request-Id"),
		StatusCode: result.StatusCode,
		FileSize:   fileInfo.Size(),
	}, nil
}
