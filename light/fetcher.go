package light

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/streadway/amqp"
)

var rabbitUrl = os.Getenv(
	"AMQP_URL",
)

type AMQP struct {
	con     *amqp.Connection
	channel *amqp.Channel
}

type Parser struct {
	bc   *LightChain
	amqp AMQP
}

type TransactionData struct {
	Receipt *types.Receipt  `json:"receipt" 	gencodec:"required"`
	Input   []byte          `json:"calldata"`
	Value   *big.Int        `json:"value"`
	Address *common.Address `json:"contract"`
	Code    []byte          `json:"contract_code"`
}

type TransactionDatas = []*TransactionData

// go run github.com/fjl/gencodec -dir light/  -type ParsedMessage parsedMessageMarshaling -out gen_parsedmessage_json.go
type ParsedMessage struct {
	Transactions TransactionDatas `json:"transactions" 	gencodec:"required"`
	Block        *types.Block     `json:"block" 		gencodec:"required"`
}

type parsedMessageMarshaling struct {
	Receipts *types.Receipts
	Block    *types.Block
}

func NewParser(bc *LightChain) (*Parser, error) {
	p := &Parser{
		bc: bc,
	}
	var err error
	var amqpClient AMQP
	if rabbitUrl == "" {
		rabbitUrl = "amqp://guest:guest@localhost:5672/"
	}

	con, err := amqp.Dial(rabbitUrl)
	if con != nil {
		log.Info("Successfully Connected to RabbitMQ Instance")
	} else {
		log.Crit("RabbitMQ Instance Is Unabled", err)
		return nil, err
	}

	channel, err := con.Channel()
	if err != nil {
		log.Crit("RabbitMQ Instance Is Unabled", err)
		return nil, err
	}

	amqpClient.con = con
	amqpClient.channel = channel
	p.amqp = amqpClient

	return p, nil
}

func (p Parser) parseBlock(block *types.Block) {

	var trans = block.Transactions()

	if len(trans) == 0 {
		return
	}

	var req ReceiptsRequest
	req.Hash = block.Hash()
	req.Number = block.NumberU64()
	req.Header = block.Header()
	req.Untrusted = true

	p.bc.odr.Retrieve(context.Background(), &req)

	collected_trans, err := p.collectData(req.Receipts, trans, block)

	var message ParsedMessage

	message.Block = block
	message.Transactions = collected_trans

	data, err := json.Marshal(req.Receipts)

	if err == nil {
		encoded := []byte(data)
		fmt.Println(len(encoded))
		route := "eth"
		p.amqp.sendMessage(encoded, route)

	} else {
		fmt.Println(err)
	}

}

func (p Parser) collectData(rs types.Receipts, txs types.Transactions, block *types.Block) (TransactionDatas, error) {
	signer := types.MakeSigner(p.bc.chainConfig, block.Number())
	var transactions TransactionDatas

	fmt.Println(len(rs))

	logIndex := uint(0)
	if len(txs) != len(rs) {
		return nil, errors.New("transaction and receipt count mismatch")
	}

	for i := 0; i < len(rs); i++ {
		// The transaction type and hash can be retrieved from the transaction itself

		var data TransactionData

		rs[i].Type = txs[i].Type()
		rs[i].TxHash = txs[i].Hash()

		// block location fields
		rs[i].BlockHash = block.Hash()
		rs[i].BlockNumber = block.Number()
		rs[i].TransactionIndex = uint(i)

		if txs[i].To() != nil {
			rs[i].ContractAddress = *txs[i].To()
		}

		// The contract address can be derived from the transaction itself
		if txs[i].To() == nil {
			// Deriving the signer is expensive, only do if it's actually needed
			from, _ := types.Sender(signer, txs[i])
			rs[i].ContractAddress = crypto.CreateAddress(from, txs[i].Nonce())
		}
		// The used gas can be calculated based on previous r
		if i == 0 {
			rs[i].GasUsed = rs[i].CumulativeGasUsed
		} else {
			rs[i].GasUsed = rs[i].CumulativeGasUsed - rs[i-1].CumulativeGasUsed
		}
		// The derived log fields can simply be set from the block and transaction
		for j := 0; j < len(rs[i].Logs); j++ {
			rs[i].Logs[j].BlockNumber = block.NumberU64()
			rs[i].Logs[j].BlockHash = block.Hash()
			rs[i].Logs[j].TxHash = rs[i].TxHash
			rs[i].Logs[j].TxIndex = uint(i)
			rs[i].Logs[j].Index = logIndex
			logIndex++
		}

		data.Receipt = rs[i]
		data.Input = txs[i].Data()
		data.Value = txs[i].Value()
		data.Address = txs[i].To()

		if txs[i].To() != nil {
			var code_req CodeRequest
			code_req.Id = StateTrieID(block.Header())
			code_req.Hash = txs[i].To().Hash()

			// p.bc.odr.Retrieve(NoOdr, &code_req)
			// data.Code = code_req.Data
			// fmt.Println(code_req)
		}
		transactions = append(transactions, &data)
	}

	return transactions, nil
}

func (r AMQP) sendMessage(msg []byte, route string) {
	r.channel.Publish(
		"amq.direct",
		route,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        msg,
		},
	)
	log.Info("Message has beed sent to queue")
}
