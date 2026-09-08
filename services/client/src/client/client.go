package client

import (
	"bufio"
	"context"
	"encoding/csv"
	"net"
	"os"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/domain"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/service"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

const BUFFER_SIZE = 512
const MAX_LINE_SIZE = 64 * 1024

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
	BatchSize  uint32
}

type Client struct {
	conn    net.Conn
	service *service.LotteryService
	config  ClientConfig
}

func NewClient(ctx context.Context, config ClientConfig) (*Client, error) {
	conn, err := connectToServer(ctx, config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{
		conn:    conn,
		service: service.NewLotteryService(conn, config.AgencyId),
		config:  config,
	}
	return client, nil
}

func connectToServer(ctx context.Context, host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond):
			}
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func (client *Client) Run(ctx context.Context) error {
	const mainAction = "lottery-round"
	defer client.conn.Close()
	defer client.closeConnOnShutdown(ctx)()

	logger.Info(mainAction, logger.InProgress, "agency-id", client.config.AgencyId)

	inputFile, fileErr := client.readInputFile()
	if fileErr != nil {
		return fileErr
	}
	defer inputFile.Close()

	outputFile, oFileErr := client.createOutputFile()
	if oFileErr != nil {
		return oFileErr
	}
	defer outputFile.Close()

	sendBetsErr := client.sendBetsBatch(ctx, inputFile)
	if sendBetsErr != nil {
		return sendBetsErr
	}

	awaitingWinnersErr := client.notifyAwaitingWinners()
	if awaitingWinnersErr != nil {
		return awaitingWinnersErr
	}

	winners, winnersErr := client.readWinners()
	if winnersErr != nil {
		return winnersErr
	}

	if storeErr := client.storeWinners(outputFile, winners); storeErr != nil {
		return storeErr
	}

	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId)

	return nil
}

func (client *Client) readInputFile() (*os.File, error) {
	const action = "open-input-file"
	const inputFileArg = "input-file"

	file, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error(action, logger.Fail, inputFileArg, client.config.InputFile)
		return nil, err
	}

	logger.Info(action, logger.Success, inputFileArg, client.config.InputFile)
	return file, nil
}

func (client *Client) createOutputFile() (*os.File, error) {
	const action = "create-output-file"
	const outputFileArg = "output-file"

	outputFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		logger.Error(action, logger.Fail, outputFileArg, client.config.OutputFile)
		return nil, err
	}

	logger.Info(action, logger.Success, outputFileArg, client.config.OutputFile)
	return outputFile, nil
}

func (client *Client) closeConnOnShutdown(ctx context.Context) func() {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			client.conn.Close()
		case <-done:
		}
	}()
	return func() { close(done) }
}

func (client *Client) sendBetsBatch(ctx context.Context, file *os.File) error {
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, BUFFER_SIZE), MAX_LINE_SIZE)

	for batchId := 1; ; batchId++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		logger.Info("read-bets-batch", logger.InProgress, "batch-id", batchId)
		bets, err := readBatch(scanner, int(client.config.BatchSize))
		if err != nil {
			return err
		}
		if len(bets) == 0 {
			break
		}

		if err := client.sendBets(bets, batchId); err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Error("read-file", logger.Fail, "input-file", client.config.InputFile)
		return err
	}
	return nil
}

func readBatch(scanner *bufio.Scanner, batchSize int) ([]domain.Bet, error) {
	var bets []domain.Bet
	for i := 0; i < batchSize && scanner.Scan(); i++ {
		bet, err := domain.ParseBetLine(scanner.Text())
		if err != nil {
			return nil, err
		}
		bets = append(bets, bet)
	}
	return bets, nil
}

func (client *Client) sendBets(bets []domain.Bet, batchId int) error {
	return client.step("send-bets", func() error {
		return client.service.SendBets(bets)
	}, "batch-id", batchId)
}

func (client *Client) notifyAwaitingWinners() error {
	return client.step("send-awaiting-winners", func() error {
		return client.service.NotifyAwaitingWinners()
	}, "agency-id", client.config.AgencyId)
}

func (client *Client) readWinners() ([]domain.Bet, error) {
	var winners []domain.Bet

	err := client.step("read-winners", func() error {
		var readErr error
		winners, readErr = client.service.ReadWinners()
		return readErr
	}, "agency-id", client.config.AgencyId)

	if err != nil {
		return nil, err
	}
	return winners, nil
}

func (client *Client) storeWinners(file *os.File, winners []domain.Bet) error {
	return client.step("store-winners", func() error {
		rows := make([][]string, 0, len(winners))
		for _, winner := range winners {
			rows = append(rows, winner.Fields())
		}

		// WriteAll hace el Flush y devuelve el error de escritura.
		return csv.NewWriter(file).WriteAll(rows)
	}, "winners-amount", len(winners))
}

func (client *Client) step(action string, fn func() error, args ...any) error {
	logger.Info(action, logger.InProgress, args...)
	if err := fn(); err != nil {
		logger.Error(action, logger.Fail, args...)
		return err
	}
	logger.Info(action, logger.Success, args...)
	return nil
}

func (client *Client) ReadBets(file *os.File) []domain.Bet {
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, BUFFER_SIZE), MAX_LINE_SIZE)

	var bets []domain.Bet
	for scanner.Scan() {
		bet, betParseErr := domain.ParseBetLine(scanner.Text())
		if betParseErr != nil {
			logger.Error("parse-bet", logger.Fail, "line", scanner.Text())
			continue
		}
		bets = append(bets, bet)
	}

	if err := scanner.Err(); err != nil {
		logger.Error("read-file", logger.Fail, "input-file", client.config.InputFile)
	}

	return bets
}
