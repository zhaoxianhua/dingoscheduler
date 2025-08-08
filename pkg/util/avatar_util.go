package util

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/net/html"
)

const baseURL = "https://huggingface.co" // 固定Hugging Face基础URL

// FetchAvatarURL 仅需传入组织名（如"hssd"），即可获取对应的头像链接
func FetchAvatarURL(orgName string) (string, error) {
	orgURL := fmt.Sprintf("%s/%s", baseURL, orgName)

	// 2. 创建HTTP客户端，模拟浏览器请求
	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // 跳过TLS验证（可选，视环境而定）
		},
	}

	req, err := http.NewRequest("GET", orgURL, nil)
	if err != nil {
		return "", fmt.Errorf("创建请求失败: %w", err)
	}

	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("请求组织页面失败（%s）: %w", orgURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("组织页面状态异常（%s）: 状态码 %d", orgURL, resp.StatusCode)
	}

	doc, err := html.Parse(resp.Body)
	if err != nil {
		return "", fmt.Errorf("解析页面HTML失败: %w", err)
	}

	var avatarURL string
	var findAvatar func(*html.Node)
	findAvatar = func(n *html.Node) {
		if avatarURL != "" {
			return
		}

		// 步骤1: 定位头像外层div容器（根据页面结构特征匹配）
		if n.Type == html.ElementNode && n.Data == "div" {
			for _, attr := range n.Attr {
				if attr.Key == "class" &&
					strings.Contains(attr.Val, "relative") &&
					strings.Contains(attr.Val, "mr-4") &&
					strings.Contains(attr.Val, "flex") &&
					(strings.Contains(attr.Val, "size-16") || strings.Contains(attr.Val, "size-20")) {

					// 步骤2: 在div内查找指向组织主页的a标签（验证title或href）
					for c := n.FirstChild; c != nil; c = c.NextSibling {
						if c.Type == html.ElementNode && c.Data == "a" {
							// 验证a标签是否指向当前组织（通过href或title）
							isTargetOrgLink := false
							for _, aAttr := range c.Attr {
								// 匹配href（如"/hssd"）或title（如"Habitat Synthetic Scenes Dataset"）
								if (aAttr.Key == "href" && strings.Trim(aAttr.Val, "/") == orgName) ||
									(aAttr.Key == "title" && strings.Contains(strings.ToLower(aAttr.Val), strings.ToLower(orgName))) {
									isTargetOrgLink = true
									break
								}
							}
							if !isTargetOrgLink {
								continue
							}

							// 步骤3: 在a标签内查找img标签，提取src属性（头像URL）
							for imgNode := c.FirstChild; imgNode != nil; imgNode = imgNode.NextSibling {
								if imgNode.Type == html.ElementNode && imgNode.Data == "img" {
									for _, imgAttr := range imgNode.Attr {
										if imgAttr.Key == "src" && imgAttr.Val != "" {
											// 验证是否为头像链接（匹配域名特征）
											if strings.Contains(imgAttr.Val, "cdn-avatars.huggingface.co") ||
												strings.Contains(imgAttr.Val, "/avatars/") ||
												strings.Contains(imgAttr.Val, orgName) {
												avatarURL = imgAttr.Val
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

		// 递归遍历所有子节点
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			findAvatar(c)
			if avatarURL != "" {
				return
			}
		}
	}
	findAvatar(doc)

	if avatarURL == "" {
		return "", fmt.Errorf("未在组织页面（%s）中找到头像元素", orgURL)
	}

	if !strings.HasPrefix(avatarURL, "http") {
		if strings.HasPrefix(avatarURL, "//") {
			avatarURL = "https:" + avatarURL // 处理//开头的协议相对路径
		} else if strings.HasPrefix(avatarURL, "/") {
			base, _ := url.Parse(baseURL)
			parsed, _ := url.Parse(avatarURL)
			avatarURL = base.ResolveReference(parsed).String()
		} else {
			parsedOrgURL, _ := url.Parse(orgURL)
			parsedSrc, _ := url.Parse(avatarURL)
			avatarURL = parsedOrgURL.ResolveReference(parsedSrc).String()
		}
	}

	return avatarURL, nil
}

func DownloadAvatar(avatarURL, saveRoot, subDir, org string) error {
	filename := fmt.Sprintf("%s_%s_avatar.jpg", subDir, org)
	savePath := filepath.Join(saveRoot, filename)

	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	resp, err := client.Get(avatarURL)
	if err != nil {
		return fmt.Errorf("下载失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("头像状态异常: %d", resp.StatusCode)
	}

	file, err := os.Create(savePath)
	if err != nil {
		return fmt.Errorf("创建文件失败: %w", err)
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return fmt.Errorf("写入文件失败: %w", err)
	}

	return nil
}
