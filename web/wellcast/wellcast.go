package wellcast

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/naveego/api/pipeline/subscriber"
	"github.com/naveego/api/types/pipeline"
)

type Subscriber struct{}

func NewSubscriber() subscriber.Subscriber {
	return &Subscriber{}
}

func (s *Subscriber) Receive(ctx subscriber.Context, shapeInfo subscriber.ShapeInfo, dataPoint pipeline.DataPoint) {
	ctx.Logger.Info("Receiving data point")
}

func (s *Subscriber) Close() {

}

func getAuthToken(ctx subscriber.Context) (string, error) {
	ctx.Logger.Info("Authenticating to Wellcast Api")

	apiURL, ok := getStringSetting(ctx.Subscriber.Settings, "apiUrl")
	if !ok {
		return "", fmt.Errorf("Expected setting for 'apiUrl' but it was not set or not a valid string.")
	}

	user, ok := getStringSetting(ctx.Subscriber.Settings, "user")
	if !ok {
		return "", fmt.Errorf("Expected setting for 'user' but it was not set or not a valid string")
	}

	password, ok := getStringSetting(ctx.Subscriber.Settings, "password")
	if !ok {
		return "", fmt.Errorf("Expected setting for 'password' but it was not set or not a valid string")
	}

	cli := http.Client{}
	authURL := fmt.Sprintf("%s/api/v2/auth/token?userName=%s&password=%s", apiURL, user, password)

	ctx.Logger.Debugf("Calling Authentication Endpoint with URL: %s", authURL)

	req, err := http.NewRequest("POST", authURL, nil)
	if err != nil {
		return "", err
	}

	resp, err := cli.Do(req)
	if resp == nil && err != nil {
		return "", err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return "", fmt.Errorf("The API returned a status code of %d", resp.StatusCode)
	}

	var respJSON map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&respJSON)
	if err != nil {
		return "", fmt.Errorf("Error decoding response: %v", err)
	}

	rawAuthToken, ok := respJSON["AuthToken"]
	if !ok {
		return "", errors.New("The response did not contain an AuthToken property")
	}

	authToken, ok := rawAuthToken.(string)
	if !ok {
		return "", errors.New("The response contained an AuthToken property that was not a valid string")
	}

	return authToken, nil

}

func getStringSetting(settings map[string]interface{}, name string) (string, bool) {

	rawValue, ok := settings[name]
	if !ok {
		return "", false
	}

	value, ok := rawValue.(string)
	if !ok {
		return "", false
	}

	return value, true
}

func writeCommonLogs(ctx subscriber.Context, action string) {
	ctx.Logger.Infof("Starting action %s", action)
	apiURL, _ := getStringSetting(ctx.Subscriber.Settings, "apiUrl")
	ctx.Logger.Infof("Using API Endpoint: %s", apiURL)
}
