package domain

type Message struct {
	Header  MessageHeader
	Payload []byte
}
type MessageType uint8

const (
	MessageTypeAck MessageType = iota + 1
	MessageTypeBet
	MessageTypeAwaitingWinners
	MessageTypeWinner
	MessageTypeFinish
)

func AckMessage() Message {
	return Message{
		Header: MessageHeader{
			Type:       MessageTypeAck,
			PayloadLen: 0,
		},
		Payload: nil,
	}
}

func BetMessage(agencyId string, bets []Bet) Message {
	var payload []byte
	for _, b := range bets {
		payload = append(payload, b.Serialize(agencyId)...)
		payload = append(payload, '\n')
	}
	return Message{
		Header: MessageHeader{
			Type:       MessageTypeBet,
			PayloadLen: uint32(len(payload)),
		},
		Payload: payload,
	}
}

func AwaitingWinnersMessage(agencyId string) Message {
	payload := []byte(agencyId)
	return Message{
		Header: MessageHeader{
			Type:       MessageTypeAwaitingWinners,
			PayloadLen: uint32(len(payload)),
		},
		Payload: payload,
	}
}
