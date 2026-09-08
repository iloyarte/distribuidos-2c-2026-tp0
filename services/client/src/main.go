package main

import (
	"fmt"
	"os"
	"strconv"

	client "github.com/7574-sistemas-distribuidos/tp-nivelador/src/client"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
)

func requireEnv(name string) (string, error) {
	value := os.Getenv(name)
	if value == "" {
		return "", fmt.Errorf("%s environment variable is required", name)
	}
	return value, nil
}

func loadConfig() (client.ClientConfig, error) {
	agencyId, err := requireEnv("AGENCY_ID")
	if err != nil {
		return client.ClientConfig{}, err
	}

	serverHost, err := requireEnv("SERVER_HOST")
	if err != nil {
		return client.ClientConfig{}, err
	}

	serverPort, err := requireEnv("SERVER_PORT")
	if err != nil {
		return client.ClientConfig{}, err
	}

	inputFile, err := requireEnv("INPUT_FILE")
	if err != nil {
		return client.ClientConfig{}, err
	}

	outputFile, err := requireEnv("OUTPUT_FILE")
	if err != nil {
		return client.ClientConfig{}, err
	}

	batchSize, err := requireEnv("BATCH_SIZE")
	if err != nil {
		return client.ClientConfig{}, err
	}

	intBatchSize, parseErr := strconv.Atoi(batchSize)
	if parseErr != nil {
		return client.ClientConfig{}, parseErr
	}


	return client.ClientConfig{
		ServerHost: serverHost,
		ServerPort: serverPort,
		AgencyId:   agencyId,
		InputFile:  inputFile,
		OutputFile: outputFile,
		BatchSize:  uint32(intBatchSize),
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		logger.Error("load-config", logger.Fail, "err", err)
		return 1
	}

	client, err := client.NewClient(config)
	if err != nil {
		logger.Error("client-new", logger.Fail, "err", err)
		return 1
	}

	if err := client.Run(); err != nil {
		logger.Error("client-run", logger.Fail, "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
