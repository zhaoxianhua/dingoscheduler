package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dingoscheduler/internal/dao"
	"dingoscheduler/internal/data"
	"dingoscheduler/internal/model"
	"dingoscheduler/pkg/config"

	"go.uber.org/zap"
	"golang.org/x/net/html"
)

var (
	configPath string
)

func init() {
	// 解析命令行参数（仅定义，需在main中执行Parse）
	flag.StringVar(&configPath, "config", "config.yaml", "配置文件路径")
}

func main() {
	// 1. 解析命令行参数
	flag.Parse()

	// 2. 读取配置文件
	conf, err := config.Scan(configPath)
	if err != nil {
		zap.S().Fatalf("读取配置文件失败：%v", err)
	}
	zap.S().Infof("成功读取配置文件，路径：%s", configPath)

	// 验证图片存储路径参数
	imageSavePath := conf.Avatar.Path
	if imageSavePath == "" {
		zap.S().Fatalf("请通过 -image-path 参数指定图片存储路径")
	}
	// 确保存储目录存在
	if err := os.MkdirAll(imageSavePath, 0755); err != nil {
		zap.S().Fatalf("创建图片存储目录失败：%v", err)
	}

	zap.S().Infof("命令行参数解析完成，配置文件路径：%s，图片存储路径：%s", configPath, imageSavePath)

	baseData, cleanup, err := data.NewBaseData(conf)
	if err != nil {
		zap.S().Fatalf("初始化基础数据（数据库连接）失败：%v", err)
	}
	defer func() {
		cleanup()
		zap.S().Infof("程序退出，已执行资源清理（释放数据库连接）")
	}()
	zap.S().Infof("基础数据（数据库连接）初始化成功")

	// 4. 初始化DAO层
	modelFileRecordDao := dao.NewModelFileRecordDao(baseData)
	organizationDao := dao.NewOrganizationDao(baseData)
	zap.S().Infof("DAO层初始化完成（ModelFileRecordDao、OrganizationDao）")

	// 5. 使用用户指定的图片存储路径
	saveRoot := imageSavePath
	zap.S().Infof("头像本地保存路径已确定（用户指定）：%s", saveRoot)

	// 6. 从数据库查询所有去重的repo
	repos, err := modelFileRecordDao.FindDistinctRepos()
	if err != nil {
		zap.S().Fatalf("从model_file_record表查询去重repo失败：%v", err)
	}
	zap.S().Infof("成功查询到 %d 个去重的repo记录", len(repos))

	// 7. 无repo时直接退出
	if len(repos) == 0 {
		zap.S().Warnf("未查询到任何repo记录，无需处理")
		return
	}

	// 8. 遍历处理每个repo
	successCount := 0 // 统计处理成功的repo数量
	for idx, repo := range repos {
		// 8.1 跳过空repo
		if repo == "" {
			zap.S().Warnf("处理第 %d/%d 个repo：检测到空repo，跳过", idx+1, len(repos))
			continue
		}
		zap.S().Infof("开始处理第 %d/%d 个repo：%s", idx+1, len(repos), repo)

		// 8.2 获取头像URL
		avatarURL, err := fetchAvatarURL(repo)
		if err != nil {
			zap.S().Errorf("处理repo [%s] 失败：获取头像URL错误，%v，跳过该repo", repo, err)
			continue
		}
		zap.S().Infof("repo [%s] 成功获取头像URL：%s", repo, avatarURL)

		// 8.3 下载头像到本地（使用用户指定路径）
		subDir := "" // 无子目录，直接存指定路径
		if err := downloadAvatar(avatarURL, saveRoot, subDir, repo); err != nil {
			zap.S().Errorf("处理repo [%s] 失败：下载头像错误，%v，跳过该repo", repo, err)
			continue
		}

		// 8.4 计算图片名称和本地完整路径
		filename := fmt.Sprintf("%s_%s_avatar.jpg", subDir, repo)
		if subDir != "" {
			filename = fmt.Sprintf("%s/%s", subDir, filename)
		}
		iconFullPath := filepath.Join(saveRoot, filename)
		zap.S().Infof("repo [%s] 头像已保存至本地：%s", repo, iconFullPath)

		// 8.5 同步到organization表（仅存储图片名称）
		org := &model.Organization{
			Name: repo,
			Icon: filename, // 数据库只存储图片名称，不存储完整路径
		}

		// 8.5.1 检查组织是否已存在
		exists, err := organizationDao.ExistsByField("name", repo)
		if err != nil {
			zap.S().Errorf("处理repo [%s] 失败：检查组织是否存在错误，%v，跳过数据库保存", repo, err)
			continue
		}

		// 8.5.2 插入或更新
		var opErr error
		if exists {
			opErr = organizationDao.UpdateByField("name", repo, org)
		} else {
			opErr = organizationDao.Insert(org)
		}
		if opErr != nil {
			zap.S().Errorf("处理repo [%s] 失败：数据库保存错误，%v，跳过", repo, opErr)
			continue
		}

		// 8.6 单个repo处理成功
		successCount++
		if exists {
			zap.S().Infof("repo [%s] 处理成功：已更新organization表记录（name=%s, 图片名称=%s）", repo, repo, filename)
		} else {
			zap.S().Infof("repo [%s] 处理成功：已插入organization表新记录（name=%s, 图片名称=%s）", repo, repo, filename)
		}
	}

	// 9. 整体处理完成日志
	zap.S().Infof("所有repo处理完成！总数量：%d，成功数量：%d，失败/跳过数量：%d",
		len(repos), successCount, len(repos)-successCount)
}

const baseURL = "https://huggingface.co" // Hugging Face 基础URL

// fetchAvatarURL 传入组织名（如"hssd"），获取对应的头像链接
func fetchAvatarURL(orgName string) (string, error) {
	orgURL := fmt.Sprintf("%s/%s", baseURL, orgName)
	zap.S().Debugf("开始请求Hugging Face组织页面：%s", orgURL) // 调试级日志，记录请求地址

	// 创建HTTP客户端（模拟浏览器+超时控制）
	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // 跳过TLS验证（视环境调整）
		},
	}

	// 构建请求（添加浏览器Header，避免被拦截）
	req, err := http.NewRequest("GET", orgURL, nil)
	if err != nil {
		return "", fmt.Errorf("创建HTTP请求失败：%w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")
	zap.S().Debugf("已构建请求，User-Agent：%s", req.Header.Get("User-Agent"))

	// 发送请求
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("请求组织页面（%s）失败：%w", orgURL, err)
	}
	defer resp.Body.Close()

	// 检查响应状态码
	if resp.StatusCode != http.StatusOK {
		zap.S().Errorf("组织页面（%s）响应异常，状态码：%d", orgURL, resp.StatusCode)
		return "", fmt.Errorf("组织页面状态异常（%s）：状态码 %d", orgURL, resp.StatusCode)
	}
	zap.S().Debugf("成功获取组织页面（%s），状态码：200", orgURL)

	// 解析HTML
	doc, err := html.Parse(resp.Body)
	if err != nil {
		return "", fmt.Errorf("解析HTML页面（%s）失败：%w", orgURL, err)
	}

	// 递归查找头像节点
	var avatarURL string
	var findAvatar func(*html.Node)
	findAvatar = func(n *html.Node) {
		if avatarURL != "" {
			return // 已找到，提前退出
		}

		// 定位头像外层div容器（通过class特征匹配）
		if n.Type == html.ElementNode && n.Data == "div" {
			for _, attr := range n.Attr {
				if attr.Key == "class" &&
					strings.Contains(attr.Val, "relative") &&
					strings.Contains(attr.Val, "mr-4") &&
					strings.Contains(attr.Val, "flex") &&
					(strings.Contains(attr.Val, "size-16") || strings.Contains(attr.Val, "size-20")) {

					zap.S().Debugf("找到疑似头像外层div，class：%s", attr.Val)

					// 在div内查找目标a标签（指向组织主页）
					for c := n.FirstChild; c != nil; c = c.NextSibling {
						if c.Type == html.ElementNode && c.Data == "a" {
							isTargetOrgLink := false
							var matchedAttr string // 记录匹配的属性（用于日志）
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

							// 在a标签内查找img（头像）
							for imgNode := c.FirstChild; imgNode != nil; imgNode = imgNode.NextSibling {
								if imgNode.Type == html.ElementNode && imgNode.Data == "img" {
									for _, imgAttr := range imgNode.Attr {
										if imgAttr.Key == "src" && imgAttr.Val != "" {
											// 验证头像URL特征
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

		// 递归遍历子节点
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			findAvatar(c)
			if avatarURL != "" {
				return
			}
		}
	}
	findAvatar(doc)

	// 检查是否找到头像
	if avatarURL == "" {
		zap.S().Errorf("在组织页面（%s）中未找到符合特征的头像节点", orgURL)
		return "", fmt.Errorf("未在组织页面（%s）中找到头像元素", orgURL)
	}

	// 处理相对路径，补全为完整URL
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

// downloadAvatar 下载头像到本地指定路径
func downloadAvatar(avatarURL, saveRoot, subDir, org string) error {
	// 构建保存路径
	filename := fmt.Sprintf("%s_%s_avatar.jpg", subDir, org)
	savePath := filepath.Join(saveRoot, filename)
	zap.S().Debugf("准备下载头像，URL：%s，保存路径：%s", avatarURL, savePath)

	// 创建HTTP客户端（同fetch逻辑，保持一致性）
	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	// 发送下载请求
	resp, err := client.Get(avatarURL)
	if err != nil {
		return fmt.Errorf("请求头像URL（%s）失败：%w", avatarURL, err)
	}
	defer resp.Body.Close()

	// 检查下载响应状态
	if resp.StatusCode != http.StatusOK {
		zap.S().Errorf("头像下载响应异常，URL：%s，状态码：%d", avatarURL, resp.StatusCode)
		return fmt.Errorf("头像下载状态异常：URL=%s，状态码=%d", avatarURL, resp.StatusCode)
	}
	zap.S().Debugf("成功获取头像文件流，URL：%s，响应大小：%d bytes", avatarURL, resp.ContentLength)

	// 创建本地文件
	file, err := os.Create(savePath)
	if err != nil {
		return fmt.Errorf("创建本地文件（%s）失败：%w", savePath, err)
	}
	defer file.Close()

	// 写入文件（复制流）
	if _, err := io.Copy(file, resp.Body); err != nil {
		return fmt.Errorf("写入本地文件（%s）失败：%w", savePath, err)
	}

	zap.S().Debugf("头像成功下载并保存：%s", savePath)
	return nil
}
