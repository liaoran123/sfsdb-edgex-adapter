// sfsDb 与 EdgeX MQTT 适配器示例（改进版）
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"sfsdb-edgex-adapter/config"
	"sfsdb-edgex-adapter/database"
	"sfsdb-edgex-adapter/server"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// EdgeX 消息结构（符合 MessageEnvelope 格式）
type EdgeXMessage struct {
	CorrelationID string          `json:"correlationId,omitempty"`
	MessageType   string          `json:"messageType,omitempty"`
	Origin        int64           `json:"origin,omitempty"`
	Payload       json.RawMessage `json:"payload"`
}

// EdgeX 事件结构
type EdgeXEvent struct {
	ID          string         `json:"id"`
	DeviceName  string         `json:"deviceName"`
	Readings    []EdgeXReading `json:"readings"`
	Origin      int64          `json:"origin"`
	ProfileName string         `json:"profileName,omitempty"`
	SourceName  string         `json:"sourceName,omitempty"`
}

// EdgeX 读数结构
type EdgeXReading struct {
	ID           string          `json:"id"`
	ResourceName string          `json:"resourceName"`
	Value        string          `json:"value"`
	Origin       int64           `json:"origin"`
	ProfileName  string          `json:"profileName,omitempty"`
	DeviceName   string          `json:"deviceName,omitempty"`
	Metadata     json.RawMessage `json:"metadata,omitempty"`
}

var appConfig *config.Config

func main() {
	// 加载配置
	var err error
	appConfig, err = config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 连接 sfsDb
	if err := database.Init(appConfig.DBPath); err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	// 函数初始化 MQTT 客户端，连接到 EdgeX 的 MQTT broker
	client, err := initMQTT()
	if err != nil {
		log.Fatalf("Failed to initialize MQTT: %v", err)
	}
	defer client.Disconnect(250)

	// 订阅 EdgeX 消息
	if err := subscribeToEdgeX(client); err != nil {
		log.Fatalf("Failed to subscribe to EdgeX messages: %v", err)
	}

	log.Println("sfsDb EdgeX adapter started successfully")

	// 启动 HTTP 服务器
	serverInstance := server.NewServer(database.Table, appConfig)
	if err := serverInstance.Start(); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}

	// 等待中断信号以优雅地关闭服务器
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit // 阻塞直到收到中断信号。
	log.Println("Shutting down adapter...")

	// 给服务器 5 秒的时间来完成正在处理的请求
	time.Sleep(5 * time.Second)

	log.Println("Adapter exited")
}

// 初始化 MQTT 客户端
// 异步处理 ：MQTT 客户端在后台运行，当收到消息时会自动调用 messageHandler() 函数
func initMQTT() (mqtt.Client, error) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(appConfig.MQTTBroker)
	opts.SetClientID(appConfig.ClientID)
	opts.SetCleanSession(true)
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(time.Second * 30)
	opts.SetDefaultPublishHandler(messageHandler())

	client := mqtt.NewClient(opts)
	token := client.Connect()
	token.Wait()
	if token.Error() != nil {
		return nil, fmt.Errorf("failed to connect to MQTT broker: %v", token.Error())
	}

	log.Printf("Connected to MQTT broker: %s", appConfig.MQTTBroker)
	return client, nil
}

// 订阅 EdgeX 消息
func subscribeToEdgeX(client mqtt.Client) error {
	token := client.Subscribe(appConfig.MQTTTopic, 1, nil)
	token.Wait()
	if token.Error() != nil {
		return fmt.Errorf("failed to subscribe to topic %s: %v", appConfig.MQTTTopic, token.Error())
	}

	log.Printf("Subscribed to topic: %s", appConfig.MQTTTopic)
	return nil
}

// 消息处理函数
func messageHandler() mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		log.Printf("Received message on topic: %s", msg.Topic())

		var edgexMsg EdgeXMessage
		if err := json.Unmarshal(msg.Payload(), &edgexMsg); err != nil {
			log.Printf("Failed to parse message: %v", err)
			return
		}

		// 解析 payload 中的事件
		var event EdgeXEvent
		if err := json.Unmarshal(edgexMsg.Payload, &event); err != nil {
			log.Printf("Failed to parse event: %v", err)
			return
		}

		// 收集所有读数，准备批量插入
		var records []*map[string]any

		// 处理每个读数
		for _, reading := range event.Readings {
			// 准备数据
			metadataStr := ""
			if reading.Metadata != nil {
				metadataStr = string(reading.Metadata)
			}

			// 将字符串值转换为浮点数
			value := 0.0
			fmt.Sscanf(reading.Value, "%f", &value)

			data := map[string]any{
				"id":         reading.ID,
				"deviceName": event.DeviceName,
				"reading":    reading.ResourceName,
				"value":      value,
				"timestamp":  int(reading.Origin / 1000000000), // 转换为秒，类型为 int
				"metadata":   metadataStr,
			}

			records = append(records, &data)
		}

		// 批量存储到 sfsDb
		if len(records) > 0 {
			_, err := database.Table.BatchInsertNoInc(records)
			if err != nil {
				log.Printf("Failed to batch store data: %v", err)
			} else {
				log.Printf("Batch stored %d readings from %s", len(records), event.DeviceName)
			}
		}
	}
}
