package tunnel

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/Jnoson/MongoShake/v2/collector/configure"
	utils "github.com/Jnoson/MongoShake/v2/common"
	"github.com/Jnoson/MongoShake/v2/tunnel/rocketmq"
	"math"
	"sync/atomic"

	LOG "github.com/vinllen/log4go"
	bson2 "github.com/vinllen/mongo-go-driver/bson"
	"os"
	"strings"
)

const (
	inputChanelSize  = 256
	outputChanelSize = 4196
)

var (
	unitTestWriteRocketmqFlag = false
	unitTestWriteRocketmqChan chan []byte
)

type rocketMqOutputLog struct {
	isEnd bool
	log   []byte
}

type RocketmqWriter struct {
	RemoteAddr string
	retryTimes int                      // write to which partition
	writer     *rocketmq.SyncWriter     // writer
	state      int64                    // state: ok, error
	encoderNr  int64                    // how many encoder
	inputChan  []chan *WMessage         // each encoder has 1 inputChan
	outputChan []chan rocketMqOutputLog // output chan length
	pushIdx    int64                    // push into which encoder
	popIdx     int64                    // pop from which encoder
}

func (tunnel *RocketmqWriter) Name() string {
	return "rocketmq"
}

func (tunnel *RocketmqWriter) Prepare() bool {
	var writer *rocketmq.SyncWriter
	var err error
	if !unitTestWriteRocketmqFlag {
		writer, err = rocketmq.NewSyncWriter(tunnel.RemoteAddr, tunnel.retryTimes)
		if err != nil {
			LOG.Critical("rocketmq prepare[%v] create writer error[%v]", tunnel.RemoteAddr, err)
			return false
		}
		LOG.Info("start rocketmq producer")
		if err := writer.Start(); err != nil {
			LOG.Critical("rocketmq prepare[%v] start writer error[%v]", tunnel.RemoteAddr, err)
			return false
		}
	}

	tunnel.writer = writer
	tunnel.state = ReplyOK
	tunnel.encoderNr = int64(math.Max(float64(4), 1))
	tunnel.inputChan = make([]chan *WMessage, tunnel.encoderNr)
	tunnel.outputChan = make([]chan rocketMqOutputLog, tunnel.encoderNr)
	tunnel.pushIdx = 0
	tunnel.popIdx = 0

	LOG.Info("%s starts: writer_thread count[%v]", tunnel, tunnel.encoderNr)

	// start encoder
	for i := 0; i < int(tunnel.encoderNr); i++ {
		tunnel.inputChan[i] = make(chan *WMessage, inputChanelSize)
		tunnel.outputChan[i] = make(chan rocketMqOutputLog, outputChanelSize)
		go tunnel.encode(i)
	}

	// start writeRocketMQ
	go tunnel.writeRocketMQ()

	return true
}

func (tunnel *RocketmqWriter) Send(message *WMessage) int64 {
	if len(message.RawLogs) == 0 || message.Tag&MsgProbe != 0 {
		return 0
	}

	encoderId := atomic.AddInt64(&tunnel.pushIdx, 1)
	tunnel.inputChan[encoderId%tunnel.encoderNr] <- message

	return tunnel.state
}

// KafkaWriter.AckRequired() is always false, return 0 directly
func (tunnel *RocketmqWriter) AckRequired() bool {
	return false
}

func (tunnel *RocketmqWriter) ParsedLogsRequired() bool {
	return false
}

func (tunnel *RocketmqWriter) String() string {
	return fmt.Sprintf("rocketmqWriter[%v] with retrytimes[%v]", tunnel.RemoteAddr, tunnel.retryTimes)
}

func (tunnel *RocketmqWriter) encode(id int) {
	for message := range tunnel.inputChan[id] {
		message.Tag |= MsgPersistent

		switch conf.Options.TunnelMessage {
		case utils.VarTunnelMessageBson:
			// write the raw oplog directly
			for i, log := range message.RawLogs {
				tunnel.outputChan[id] <- rocketMqOutputLog{
					isEnd: i == len(log)-1,
					log:   log,
				}
			}
		case utils.VarTunnelMessageJson:
			for i, log := range message.ParsedLogs {
				// json marshal
				var encode []byte
				var err error
				if conf.Options.TunnelJsonFormat == "" {
					encode, err = json.Marshal(log.ParsedLog)
					if err != nil {
						if strings.Contains(err.Error(), "unsupported value:") {
							LOG.Error("%s json marshal data[%v] meets unsupported value[%v], skip current oplog",
								tunnel, log.ParsedLog, err)
							continue
						} else {
							// should panic
							LOG.Crashf("%s json marshal data[%v] error[%v]", tunnel, log.ParsedLog, err)
							tunnel.state = ReplyServerFault
						}
					}
				} else if conf.Options.TunnelJsonFormat == "canonical_extended_json" {
					encode, err = bson2.MarshalExtJSON(log.ParsedLog, true, true)
					if err != nil {
						// should panic
						LOG.Crashf("%s json marshal data[%v] error[%v]", tunnel, log.ParsedLog, err)
						tunnel.state = ReplyServerFault
					}
				} else {
					LOG.Crashf("unknown tunnel.json.format[%v]", conf.Options.TunnelJsonFormat)
				}

				tunnel.outputChan[id] <- rocketMqOutputLog{
					isEnd: i == len(message.ParsedLogs)-1,
					log:   encode,
				}
			}
		case utils.VarTunnelMessageRaw:
			byteBuffer := bytes.NewBuffer([]byte{})
			// checksum
			binary.Write(byteBuffer, binary.BigEndian, uint32(message.Checksum))
			// tag
			binary.Write(byteBuffer, binary.BigEndian, uint32(message.Tag))
			// shard
			binary.Write(byteBuffer, binary.BigEndian, uint32(message.Shard))
			// compressor
			binary.Write(byteBuffer, binary.BigEndian, uint32(message.Compress))
			// serialize log count
			binary.Write(byteBuffer, binary.BigEndian, uint32(len(message.RawLogs)))

			// serialize logs
			for i, log := range message.RawLogs {
				binary.Write(byteBuffer, binary.BigEndian, uint32(len(log)))
				binary.Write(byteBuffer, binary.BigEndian, log)

				tunnel.outputChan[id] <- rocketMqOutputLog{
					isEnd: i == len(message.ParsedLogs)-1,
					log:   byteBuffer.Bytes(),
				}
			}

		default:
			LOG.Crash("%s unknown tunnel.message type: ", tunnel, conf.Options.TunnelMessage)
		}
	}
}

func (tunnel *RocketmqWriter) writeRocketMQ() {
	// debug
	var debugF *os.File
	var err error
	if conf.Options.IncrSyncTunnelRocketMQDebug != "" {
		fileName := fmt.Sprintf("%s-%d", conf.Options.IncrSyncTunnelRocketMQDebug, tunnel.retryTimes)
		if _, err := os.Stat(fileName); os.IsNotExist(err) {
			if debugF, err = os.Create(fileName); err != nil {
				LOG.Crashf("%s create rocketmq debug file[%v] failed: %v", tunnel, fileName, err)
			}
		} else {
			if debugF, err = os.OpenFile(fileName, os.O_RDWR, 0666); err != nil {
				LOG.Crashf("%s open rocketmq debug file[%v] failed: %v", tunnel, fileName, err)
			}
		}
		defer debugF.Close()
	}

	for {
		tunnel.popIdx = (tunnel.popIdx + 1) % tunnel.encoderNr
		// read chan
		for data := range tunnel.outputChan[tunnel.popIdx] {
			if unitTestWriteRocketmqFlag {
				// unit test only
				unitTestWriteKafkaChan <- data.log
			} else if conf.Options.IncrSyncTunnelRocketMQDebug != "" {
				if _, err = debugF.Write(data.log); err != nil {
					LOG.Crashf("%s write to kafka debug file failed: %v, input data: %s", tunnel, err, data.log)
				}
				debugF.Write([]byte{10})
			} else {
				if err = tunnel.writer.SimpleWrite(data.log); err != nil {
					LOG.Error("%s send [%v] with type[%v] error[%v]", tunnel, tunnel.RemoteAddr,
						conf.Options.TunnelMessage, err)
					tunnel.state = ReplyError
				}
			}

			if data.isEnd {
				break
			}
		}
	}
}
