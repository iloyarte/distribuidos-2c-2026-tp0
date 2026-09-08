package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

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
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()

	config, err := loadConfig()
	if err != nil {
		logger.Error("load-config", logger.Fail, "err", err)
		return 1
	}

	client, err := client.NewClient(ctx, config)
	if err != nil {
		return exitCodeFor(ctx, "client-new", err)
	}

	if err := client.Run(ctx); err != nil {
		return exitCodeFor(ctx, "client-run", err)
	}
	return 0
}

func exitCodeFor(ctx context.Context, action string, err error) int {
	if ctx.Err() != nil {
		logger.Info("client-shutdown", logger.Success)
		return 0
	}
	logger.Error(action, logger.Fail, "err", err)
	return 1
}

func main() {
	os.Exit(run())
}
