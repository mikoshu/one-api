package controller

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/songquanpeng/one-api/common/config"
	"github.com/songquanpeng/one-api/common/ctxkey"
	"github.com/songquanpeng/one-api/common/helper"
	"github.com/songquanpeng/one-api/common/logger"
	"github.com/songquanpeng/one-api/common/message"
	"github.com/songquanpeng/one-api/middleware"
	"github.com/songquanpeng/one-api/model"
	"github.com/songquanpeng/one-api/monitor"
	"github.com/songquanpeng/one-api/relay"
	"github.com/songquanpeng/one-api/relay/adaptor/openai"
	"github.com/songquanpeng/one-api/relay/channeltype"
	"github.com/songquanpeng/one-api/relay/controller"
	"github.com/songquanpeng/one-api/relay/meta"
	relaymodel "github.com/songquanpeng/one-api/relay/model"
	"github.com/songquanpeng/one-api/relay/relaymode"
)

func buildTestRequest(model string) *relaymodel.GeneralOpenAIRequest {
	if model == "" {
		model = "gpt-3.5-turbo"
	}
	testRequest := &relaymodel.GeneralOpenAIRequest{
		Model: model,
	}
	testMessage := relaymodel.Message{
		Role:    "user",
		Content: config.TestPrompt,
	}
	testRequest.Messages = append(testRequest.Messages, testMessage)
	return testRequest
}

func parseTestResponse(resp string) (*openai.TextResponse, string, error) {
	var response openai.TextResponse
	err := json.Unmarshal([]byte(resp), &response)
	if err != nil {
		return nil, "", err
	}
	if len(response.Choices) == 0 {
		return nil, "", errors.New("response has no choices")
	}
	stringContent, ok := response.Choices[0].Content.(string)
	if !ok {
		return nil, "", errors.New("response content is not string")
	}
	return &response, stringContent, nil
}

func testChannel(ctx context.Context, channel *model.Channel, request *relaymodel.GeneralOpenAIRequest) (responseMessage string, err error, openaiErr *relaymodel.Error) {
	startTime := time.Now()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = &http.Request{
		Method: "POST",
		URL:    &url.URL{Path: "/v1/chat/completions"},
		Body:   nil,
		Header: make(http.Header),
	}
	c.Request.Header.Set("Authorization", "Bearer "+channel.Key)
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set(ctxkey.Channel, channel.Type)
	c.Set(ctxkey.BaseURL, channel.GetBaseURL())
	cfg, _ := channel.LoadConfig()
	c.Set(ctxkey.Config, cfg)
	middleware.SetupContextForSelectedChannel(c, channel, "")
	meta := meta.GetByContext(c)
	apiType := channeltype.ToAPIType(channel.Type)
	adaptor := relay.GetAdaptor(apiType)
	if adaptor == nil {
		return "", fmt.Errorf("invalid api type: %d, adaptor is nil", apiType), nil
	}
	adaptor.Init(meta)
	modelName := request.Model
	modelMap := channel.GetModelMapping()
	if modelName == "" || !strings.Contains(channel.Models, modelName) {
		modelNames := strings.Split(channel.Models, ",")
		if len(modelNames) > 0 {
			modelName = modelNames[0]
		}
	}
	if modelMap != nil && modelMap[modelName] != "" {
		modelName = modelMap[modelName]
	}
	meta.OriginModelName, meta.ActualModelName = request.Model, modelName
	request.Model = modelName
	convertedRequest, err := adaptor.ConvertRequest(c, relaymode.ChatCompletions, request)
	if err != nil {
		return "", err, nil
	}
	jsonData, err := json.Marshal(convertedRequest)
	if err != nil {
		return "", err, nil
	}
	defer func() {
		logContent := fmt.Sprintf("渠道 %s 测试成功，响应：%s", channel.Name, responseMessage)
		if err != nil || openaiErr != nil {
			errorMessage := ""
			if err != nil {
				errorMessage = err.Error()
			} else {
				errorMessage = openaiErr.Message
			}
			logContent = fmt.Sprintf("渠道 %s 测试失败，错误：%s", channel.Name, errorMessage)
		}
		go model.RecordTestLog(ctx, &model.Log{
			ChannelId:   channel.Id,
			ModelName:   modelName,
			Content:     logContent,
			ElapsedTime: helper.CalcElapsedTime(startTime),
		})
	}()
	logger.SysLog(string(jsonData))
	requestBody := bytes.NewBuffer(jsonData)
	c.Request.Body = io.NopCloser(requestBody)
	resp, err := adaptor.DoRequest(c, meta, requestBody)
	if err != nil {
		return "", err, nil
	}
	if resp != nil && resp.StatusCode != http.StatusOK {
		err := controller.RelayErrorHandler(resp)
		errorMessage := err.Error.Message
		if errorMessage != "" {
			errorMessage = ", error message: " + errorMessage
		}
		return "", fmt.Errorf("http status code: %d%s", resp.StatusCode, errorMessage), &err.Error
	}
	usage, respErr := adaptor.DoResponse(c, resp, meta)
	if respErr != nil {
		return "", fmt.Errorf("%s", respErr.Error.Message), &respErr.Error
	}
	if usage == nil {
		return "", errors.New("usage is nil"), nil
	}
	rawResponse := w.Body.String()
	_, responseMessage, err = parseTestResponse(rawResponse)
	if err != nil {
		logger.SysError(fmt.Sprintf("failed to parse error: %s, \nresponse: %s", err.Error(), rawResponse))
		return "", err, nil
	}
	result := w.Result()
	// print result.Body
	respBody, err := io.ReadAll(result.Body)
	if err != nil {
		return "", err, nil
	}
	logger.SysLog(fmt.Sprintf("testing channel #%d, response: \n%s", channel.Id, string(respBody)))
	return responseMessage, nil, nil
}

func TestChannel(c *gin.Context) {
	ctx := c.Request.Context()
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"success": false,
			"message": err.Error(),
		})
		return
	}
	channel, err := model.GetChannelById(id, true)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"success": false,
			"message": err.Error(),
		})
		return
	}
	modelName := c.Query("model")
	testRequest := buildTestRequest(modelName)
	tik := time.Now()
	responseMessage, err, _ := testChannel(ctx, channel, testRequest)
	tok := time.Now()
	milliseconds := tok.Sub(tik).Milliseconds()
	if err != nil {
		milliseconds = 0
	}
	go channel.UpdateResponseTime(milliseconds)
	consumedTime := float64(milliseconds) / 1000.0
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"success":   false,
			"message":   err.Error(),
			"time":      consumedTime,
			"modelName": modelName,
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"success":   true,
		"message":   responseMessage,
		"time":      consumedTime,
		"modelName": modelName,
	})
	return
}

type InteractiveChannelTestRequest struct {
	Model  string `json:"model"`
	Mode   string `json:"mode"`
	Prompt string `json:"prompt"`
	Image  string `json:"image"` // base64 or data URL, used by image edits
}

// TestChannelInteractive runs a one-off administrator test without consuming
// a user's quota. It returns the request fields and the upstream response so
// the dashboard can show exactly what was tested.
func TestChannelInteractive(c *gin.Context) {
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"success": false, "message": err.Error()})
		return
	}
	channel, err := model.GetChannelById(id, true)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"success": false, "message": err.Error()})
		return
	}
	var input InteractiveChannelTestRequest
	if err = c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusOK, gin.H{"success": false, "message": err.Error()})
		return
	}
	if input.Model == "" {
		input.Model = strings.Split(channel.Models, ",")[0]
	}
	if input.Prompt == "" {
		input.Prompt = config.TestPrompt
	}
	if input.Mode == "" {
		input.Mode = "chat"
	}
	start := time.Now()
	var response string
	var rawResponse string
	var imageURLs []string
	if input.Mode == "image_generation" || input.Mode == "image_edit" {
		response, rawResponse, imageURLs, err = testImageChannel(c.Request.Context(), channel, &input)
	} else {
		request := buildTestRequest(input.Model)
		request.Messages[0].Content = input.Prompt
		response, err, _ = testChannel(c.Request.Context(), channel, request)
		rawResponse = response
	}
	result := gin.H{
		"success":     err == nil,
		"time":        time.Since(start).Seconds(),
		"modelName":   input.Model,
		"mode":        input.Mode,
		"request":     input.Prompt,
		"response":    response,
		"rawResponse": rawResponse,
		"imageUrls":   imageURLs,
	}
	if err != nil {
		result["message"] = err.Error()
	} else {
		result["message"] = ""
	}
	c.JSON(http.StatusOK, result)
}

func testImageChannel(ctx context.Context, channel *model.Channel, input *InteractiveChannelTestRequest) (string, string, []string, error) {
	path := "/v1/images/generations"
	if input.Mode == "image_edit" {
		path = "/v1/images/edits"
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = &http.Request{Method: http.MethodPost, URL: &url.URL{Path: path}, Header: make(http.Header)}
	c.Request.Header.Set("Authorization", "Bearer "+channel.Key)
	c.Set(ctxkey.Channel, channel.Type)
	c.Set(ctxkey.BaseURL, channel.GetBaseURL())
	cfg, _ := channel.LoadConfig()
	c.Set(ctxkey.Config, cfg)
	middleware.SetupContextForSelectedChannel(c, channel, "")
	meta := meta.GetByContext(c)
	meta.OriginModelName = input.Model
	meta.ActualModelName = input.Model
	if mapped := channel.GetModelMapping(); mapped != nil && mapped[input.Model] != "" {
		meta.ActualModelName = mapped[input.Model]
	}
	apiType := channeltype.ToAPIType(channel.Type)
	adaptor := relay.GetAdaptor(apiType)
	if adaptor == nil {
		return "", "", nil, fmt.Errorf("invalid api type: %d, adaptor is nil", apiType)
	}
	adaptor.Init(meta)
	var body io.Reader
	if input.Mode == "image_edit" {
		var buffer bytes.Buffer
		writer := multipart.NewWriter(&buffer)
		_ = writer.WriteField("model", meta.ActualModelName)
		_ = writer.WriteField("prompt", input.Prompt)
		imageData := input.Image
		if comma := strings.Index(imageData, ","); comma >= 0 {
			imageData = imageData[comma+1:]
		}
		decoded, err := base64.StdEncoding.DecodeString(imageData)
		if err != nil {
			return "", "", nil, fmt.Errorf("图片数据无效: %w", err)
		}
		part, err := writer.CreateFormFile("image", "test.png")
		if err != nil {
			return "", "", nil, err
		}
		if _, err = part.Write(decoded); err != nil {
			return "", "", nil, err
		}
		if err = writer.Close(); err != nil {
			return "", "", nil, err
		}
		c.Request.Header.Set("Content-Type", writer.FormDataContentType())
		requestBytes := buffer.Bytes()
		c.Request.Body = io.NopCloser(bytes.NewReader(requestBytes))
		body = bytes.NewReader(requestBytes)
	} else {
		payload, err := json.Marshal(relaymodel.ImageRequest{Model: meta.ActualModelName, Prompt: input.Prompt, N: 1, Size: "1024x1024"})
		if err != nil {
			return "", "", nil, err
		}
		c.Request.Header.Set("Content-Type", "application/json")
		c.Request.Body = io.NopCloser(bytes.NewReader(payload))
		body = bytes.NewReader(payload)
	}
	resp, err := adaptor.DoRequest(c, meta, body)
	if err != nil {
		return "", "", nil, err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		upstreamErr := controller.RelayErrorHandler(resp)
		return "", "", nil, fmt.Errorf("http status code: %d: %s", resp.StatusCode, upstreamErr.Error.Message)
	}
	if _, responseErr := adaptor.DoResponse(c, resp, meta); responseErr != nil {
		return "", "", nil, errors.New(responseErr.Error.Message)
	}
	raw := w.Body.String()
	var parsed openai.ImageResponse
	if err = json.Unmarshal([]byte(raw), &parsed); err != nil {
		return raw, raw, nil, nil
	}
	urls := make([]string, 0, len(parsed.Data))
	for _, item := range parsed.Data {
		if item.Url != "" {
			urls = append(urls, item.Url)
		} else if item.B64Json != "" {
			urls = append(urls, "data:image/png;base64,"+item.B64Json)
		}
	}
	return raw, raw, urls, nil
}

var testAllChannelsLock sync.Mutex
var testAllChannelsRunning bool = false

func testChannels(ctx context.Context, notify bool, scope string) error {
	if config.RootUserEmail == "" {
		config.RootUserEmail = model.GetRootUserEmail()
	}
	testAllChannelsLock.Lock()
	if testAllChannelsRunning {
		testAllChannelsLock.Unlock()
		return errors.New("测试已在运行中")
	}
	testAllChannelsRunning = true
	testAllChannelsLock.Unlock()
	channels, err := model.GetAllChannels(0, 0, scope)
	if err != nil {
		return err
	}
	var disableThreshold = int64(config.ChannelDisableThreshold * 1000)
	if disableThreshold == 0 {
		disableThreshold = 10000000 // a impossible value
	}
	go func() {
		for _, channel := range channels {
			isChannelEnabled := channel.Status == model.ChannelStatusEnabled
			tik := time.Now()
			testRequest := buildTestRequest("")
			_, err, openaiErr := testChannel(ctx, channel, testRequest)
			tok := time.Now()
			milliseconds := tok.Sub(tik).Milliseconds()
			if isChannelEnabled && milliseconds > disableThreshold {
				err = fmt.Errorf("响应时间 %.2fs 超过阈值 %.2fs", float64(milliseconds)/1000.0, float64(disableThreshold)/1000.0)
				if config.AutomaticDisableChannelEnabled {
					monitor.DisableChannel(channel.Id, channel.Name, err.Error())
				} else {
					_ = message.Notify(message.ByAll, fmt.Sprintf("渠道 %s （%d）测试超时", channel.Name, channel.Id), "", err.Error())
				}
			}
			if isChannelEnabled && monitor.ShouldDisableChannel(openaiErr, -1) {
				monitor.DisableChannel(channel.Id, channel.Name, err.Error())
			}
			if !isChannelEnabled && monitor.ShouldEnableChannel(err, openaiErr) {
				monitor.EnableChannel(channel.Id, channel.Name)
			}
			channel.UpdateResponseTime(milliseconds)
			time.Sleep(config.RequestInterval)
		}
		testAllChannelsLock.Lock()
		testAllChannelsRunning = false
		testAllChannelsLock.Unlock()
		if notify {
			err := message.Notify(message.ByAll, "渠道测试完成", "", "渠道测试完成，如果没有收到禁用通知，说明所有渠道都正常")
			if err != nil {
				logger.SysError(fmt.Sprintf("failed to send email: %s", err.Error()))
			}
		}
	}()
	return nil
}

func TestChannels(c *gin.Context) {
	ctx := c.Request.Context()
	scope := c.Query("scope")
	if scope == "" {
		scope = "all"
	}
	err := testChannels(ctx, true, scope)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"success": false,
			"message": err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "",
	})
	return
}

func AutomaticallyTestChannels(frequency int) {
	ctx := context.Background()
	for {
		time.Sleep(time.Duration(frequency) * time.Minute)
		logger.SysLog("testing all channels")
		_ = testChannels(ctx, false, "all")
		logger.SysLog("channel test finished")
	}
}
