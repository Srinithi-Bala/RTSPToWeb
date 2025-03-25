package main

import (
"encoding/json"
	"time"

	webrtc "github.com/deepch/vdk/format/webrtcv3"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// Define IceUrl struct
type IceUrl struct {
	StunUrl      string `json:"stunUrl"`
	TurnUrl      string `json:"turn_url"`
	TurnUsername string `json:"turn_username"`
	TurnPassword string `json:"turn_password"`
}

func HTTPAPIServerStreamWebRTC(c *gin.Context) {
	requestLogger := log.WithFields(logrus.Fields{
		"module":  "http_webrtc",
		"stream":  c.Param("uuid"),
		"channel": c.Param("channel"),
		"func":    "HTTPAPIServerStreamWebRTC",
	})

	// Parse ICE configuration from request
	iceUrl := c.PostForm("ice_data")
	var iceUrlResponse IceUrl

	err := json.Unmarshal([]byte(iceUrl), &iceUrlResponse)
	if err != nil {
		c.IndentedJSON(400, Message{Status: 0, Payload: "Invalid ICE server configuration"})
		requestLogger.WithFields(logrus.Fields{"call": "Unmarshal"}).Errorln("Error parsing ICE JSON:", err)
		return
	}

	// Determine ICE server settings
	var iceServers []string
	var iceUsername, iceCredential string

	if iceUrlResponse.StunUrl != "" {
		iceServers = []string{iceUrlResponse.StunUrl}
	} else {
		iceServers = []string{iceUrlResponse.TurnUrl}
		iceUsername = iceUrlResponse.TurnUsername
		iceCredential = iceUrlResponse.TurnPassword
	}
	// Validate stream existence
	if !Storage.StreamChannelExist(c.Param("uuid"), c.Param("channel")) {
		c.IndentedJSON(500, Message{Status: 0, Payload: ErrorStreamNotFound.Error()})
		requestLogger.WithFields(logrus.Fields{"call": "StreamChannelExist"}).Errorln(ErrorStreamNotFound.Error())
		return
	}

	// Perform remote authorization
	if !RemoteAuthorization("WebRTC", c.Param("uuid"), c.Param("channel"), c.Query("token"), c.ClientIP()) {
		requestLogger.WithFields(logrus.Fields{"call": "RemoteAuthorization"}).Errorln(ErrorStreamUnauthorized.Error())
		c.IndentedJSON(401, Message{Status: 0, Payload: "Unauthorized"})
		return
	}

	// Start stream
	Storage.StreamChannelRun(c.Param("uuid"), c.Param("channel"))

	// Fetch codecs
	codecs, err := Storage.StreamChannelCodecs(c.Param("uuid"), c.Param("channel"))
	if err != nil {
		c.IndentedJSON(500, Message{Status: 0, Payload: err.Error()})
		requestLogger.WithFields(logrus.Fields{"call": "StreamCodecs"}).Errorln(err.Error())
		return
	}

	// Create WebRTC muxer with dynamic ICE details
	muxerWebRTC := webrtc.NewMuxer(webrtc.Options{
		ICEServers:   iceServers,
		ICEUsername:  iceUsername,
		ICECredential: iceCredential,
		PortMin:      Storage.ServerWebRTCPortMin(),
		PortMax:      Storage.ServerWebRTCPortMax(),
	})

	// Write WebRTC header
	answer, err := muxerWebRTC.WriteHeader(codecs, c.PostForm("data"))
	if err != nil {
		c.IndentedJSON(400, Message{Status: 0, Payload: err.Error()})
		requestLogger.WithFields(logrus.Fields{"call": "WriteHeader"}).Errorln(err.Error())
		return
	}

	_, err = c.Writer.Write([]byte(answer))
	if err != nil {
		c.IndentedJSON(400, Message{Status: 0, Payload: err.Error()})
		requestLogger.WithFields(logrus.Fields{"call": "Write"}).Errorln(err.Error())
		return
	}

	// Handle streaming in a goroutine
	go func() {
		cid, ch, _, err := Storage.ClientAdd(c.Param("uuid"), c.Param("channel"), WEBRTC)
		if err != nil {
			c.IndentedJSON(400, Message{Status: 0, Payload: err.Error()})
			requestLogger.WithFields(logrus.Fields{"call": "ClientAdd"}).Errorln(err.Error())
			return
		}
		defer Storage.ClientDelete(c.Param("uuid"), cid, c.Param("channel"))

		var videoStart bool
		noVideo := time.NewTimer(10 * time.Second)

		for {
			select {
			case <-noVideo.C:
				requestLogger.WithFields(logrus.Fields{"call": "ErrorStreamNoVideo"}).Errorln(ErrorStreamNoVideo.Error())
				return
			case pck := <-ch:
				if pck.IsKeyFrame {
					noVideo.Reset(10 * time.Second)
					videoStart = true
				}
				if !videoStart {
					continue
				}
				err = muxerWebRTC.WritePacket(*pck)
				if err != nil {
					requestLogger.WithFields(logrus.Fields{"call": "WritePacket"}).Errorln(err.Error())
					return
				}
			}
		}
	}()
}


