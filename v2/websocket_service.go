package binance

import (
	"fmt"
	"strings"
	"time"

	stdjson "encoding/json"
)

// Endpoints
const (
	baseWsMainURL    = "wss://fstream.binance.com/ws"
	baseWsTestnetURL = "wss://testnet.binance.vision/ws"
	// re point to f steam to have all the necessary parameters (specifically pu value)
	baseCombinedMainURL    = "wss://fstream.binance.com/stream?streams="
	baseCombinedTestnetURL = "wss://testnet.binance.vision/stream?streams="
)

var (
	// WebsocketTimeout is an interval for sending ping/pong messages if WebsocketKeepalive is enabled
	WebsocketTimeout = time.Second * 60
	// WebsocketKeepalive enables sending ping/pong messages to check the connection stability
	WebsocketKeepalive = false
)

// getWsEndpoint return the base endpoint of the WS according the UseTestnet flag
func getWsEndpoint() string {
	if UseTestnet {
		return baseWsTestnetURL
	}
	return baseWsMainURL
}

// getCombinedEndpoint return the base endpoint of the combined stream according the UseTestnet flag
func getCombinedEndpoint() string {
	if UseTestnet {
		return baseCombinedTestnetURL
	}
	return baseCombinedMainURL
}

// WsPartialDepthEvent define websocket partial depth book event
type WsPartialDepthEvent struct {
	Symbol       string
	LastUpdateID int64 `json:"lastUpdateId"`
	Bids         []Bid `json:"bids"`
	Asks         []Ask `json:"asks"`
}

// WsPartialDepthHandler handle websocket partial depth event
type WsPartialDepthHandler func(event *WsPartialDepthEvent)

// WsPartialDepthServe serve websocket partial depth handler with a symbol, using 1sec updates
func WsPartialDepthServe(symbol string, levels string, handler WsPartialDepthHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@depth%s", getWsEndpoint(), strings.ToLower(symbol), levels)
	return wsPartialDepthServe(endpoint, symbol, handler, errHandler)
}

// WsPartialDepthServe100Ms serve websocket partial depth handler with a symbol, using 100msec updates
func WsPartialDepthServe100Ms(symbol string, levels string, handler WsPartialDepthHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@depth%s@100ms", getWsEndpoint(), strings.ToLower(symbol), levels)
	return wsPartialDepthServe(endpoint, symbol, handler, errHandler)
}

// WsPartialDepthServe serve websocket partial depth handler with a symbol
func wsPartialDepthServe(endpoint string, symbol string, handler WsPartialDepthHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		j, err := newJSON(message)
		if err != nil {
			errHandler(err)
			return
		}
		event := new(WsPartialDepthEvent)
		event.Symbol = symbol
		event.LastUpdateID = j.Get("lastUpdateId").MustInt64()
		bidsLen := len(j.Get("bids").MustArray())
		event.Bids = make([]Bid, bidsLen)
		for i := 0; i < bidsLen; i++ {
			item := j.Get("bids").GetIndex(i)
			event.Bids[i] = Bid{
				Price:    item.GetIndex(0).MustString(),
				Quantity: item.GetIndex(1).MustString(),
			}
		}
		asksLen := len(j.Get("asks").MustArray())
		event.Asks = make([]Ask, asksLen)
		for i := 0; i < asksLen; i++ {
			item := j.Get("asks").GetIndex(i)
			event.Asks[i] = Ask{
				Price:    item.GetIndex(0).MustString(),
				Quantity: item.GetIndex(1).MustString(),
			}
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsCombinedPartialDepthServe is similar to WsPartialDepthServe, but it for multiple symbols
func WsCombinedPartialDepthServe(symbolLevels map[string]string, handler WsPartialDepthHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := getCombinedEndpoint()
	for s, l := range symbolLevels {
		endpoint += fmt.Sprintf("%s@depth%s", strings.ToLower(s), l) + "/"
	}
	endpoint = endpoint[:len(endpoint)-1]
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		j, err := newJSON(message)
		if err != nil {
			errHandler(err)
			return
		}
		event := new(WsPartialDepthEvent)
		stream := j.Get("stream").MustString()
		symbol := strings.Split(stream, "@")[0]
		event.Symbol = strings.ToUpper(symbol)
		data := j.Get("data").MustMap()
		event.LastUpdateID, _ = data["lastUpdateId"].(stdjson.Number).Int64()
		bidsLen := len(data["bids"].([]interface{}))
		event.Bids = make([]Bid, bidsLen)
		for i := 0; i < bidsLen; i++ {
			item := data["bids"].([]interface{})[i].([]interface{})
			event.Bids[i] = Bid{
				Price:    item[0].(string),
				Quantity: item[1].(string),
			}
		}
		asksLen := len(data["asks"].([]interface{}))
		event.Asks = make([]Ask, asksLen)
		for i := 0; i < asksLen; i++ {

			item := data["asks"].([]interface{})[i].([]interface{})
			event.Asks[i] = Ask{
				Price:    item[0].(string),
				Quantity: item[1].(string),
			}
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsDepthHandler handle websocket depth event
type WsDepthHandler func(event *WsDepthEvent)

// WsDepthServe serve websocket depth handler with a symbol, using 1sec updates
func WsDepthServe(symbol string, handler WsDepthHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@depth", getWsEndpoint(), strings.ToLower(symbol))
	return wsDepthServe(endpoint, handler, errHandler)
}

// WsDepthServe100Ms serve websocket depth handler with a symbol, using 100msec updates
func WsDepthServe100Ms(symbol string, handler WsDepthHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@depth@100ms", getWsEndpoint(), strings.ToLower(symbol))
	return wsDepthServe(endpoint, handler, errHandler)
}

// WsDepthServe serve websocket depth handler with an arbitrary endpoint address
func wsDepthServe(endpoint string, handler WsDepthHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		j, err := newJSON(message)
		if err != nil {
			errHandler(err)
			return
		}
		event := new(WsDepthEvent)
		event.Event = j.Get("e").MustString()
		event.Time = j.Get("E").MustInt64()
		event.Symbol = j.Get("s").MustString()
		event.LastUpdateID = j.Get("u").MustInt64()
		event.FirstUpdateID = j.Get("U").MustInt64()

		if j.Get("pu") != nil {
			event.LastUpdateIDInLastStream = j.Get("pu").MustInt64()
		}

		bidsLen := len(j.Get("b").MustArray())
		event.Bids = make([]Bid, bidsLen)
		for i := 0; i < bidsLen; i++ {
			item := j.Get("b").GetIndex(i)
			event.Bids[i] = Bid{
				Price:    item.GetIndex(0).MustString(),
				Quantity: item.GetIndex(1).MustString(),
			}
		}
		asksLen := len(j.Get("a").MustArray())
		event.Asks = make([]Ask, asksLen)
		for i := 0; i < asksLen; i++ {
			item := j.Get("a").GetIndex(i)
			event.Asks[i] = Ask{
				Price:    item.GetIndex(0).MustString(),
				Quantity: item.GetIndex(1).MustString(),
			}
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsDepthEvent define websocket depth event
type WsDepthEvent struct {
	Event                    string `json:"e"`
	Time                     int64  `json:"E"`
	Symbol                   string `json:"s"`
	LastUpdateID             int64  `json:"u"`
	FirstUpdateID            int64  `json:"U"`
	LastUpdateIDInLastStream int64  `json:"pu"`
	Bids                     []Bid  `json:"b"`
	Asks                     []Ask  `json:"a"`
}

// WsCombinedDepthServe is similar to WsDepthServe, but it for multiple symbols
func WsCombinedDepthServe(symbols []string, handler WsDepthHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := getCombinedEndpoint()
	for _, s := range symbols {
		endpoint += fmt.Sprintf("%s@depth", strings.ToLower(s)) + "/"
	}
	endpoint = endpoint[:len(endpoint)-1]
	return wsCombinedDepthServe(endpoint, handler, errHandler)
}

func WsCombinedDepthServe100Ms(symbols []string, handler WsDepthHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := getCombinedEndpoint()
	for _, s := range symbols {
		endpoint += fmt.Sprintf("%s@depth@100ms", strings.ToLower(s)) + "/"
	}
	endpoint = endpoint[:len(endpoint)-1]
	return wsCombinedDepthServe(endpoint, handler, errHandler)
}

func wsCombinedDepthServe(endpoint string, handler WsDepthHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		j, err := newJSON(message)
		if err != nil {
			errHandler(err)
			return
		}
		event := new(WsDepthEvent)
		stream := j.Get("stream").MustString()
		symbol := strings.Split(stream, "@")[0]
		event.Symbol = strings.ToUpper(symbol)
		data := j.Get("data").MustMap()
		event.Event = data["e"].(string)
		event.Time, _ = data["E"].(stdjson.Number).Int64()
		event.LastUpdateID, _ = data["u"].(stdjson.Number).Int64()
		event.FirstUpdateID, _ = data["U"].(stdjson.Number).Int64()

		if v, found := data["pu"]; found {
			event.LastUpdateIDInLastStream, _ = v.(stdjson.Number).Int64()
		}

		bidsLen := len(data["b"].([]interface{}))
		event.Bids = make([]Bid, bidsLen)
		for i := 0; i < bidsLen; i++ {
			item := data["b"].([]interface{})[i].([]interface{})
			event.Bids[i] = Bid{
				Price:    item[0].(string),
				Quantity: item[1].(string),
			}
		}
		asksLen := len(data["a"].([]interface{}))
		event.Asks = make([]Ask, asksLen)
		for i := 0; i < asksLen; i++ {

			item := data["a"].([]interface{})[i].([]interface{})
			event.Asks[i] = Ask{
				Price:    item[0].(string),
				Quantity: item[1].(string),
			}
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsKlineHandler handle websocket kline event
type WsKlineHandler func(event *WsKlineEvent)

// WsCombinedKlineServe is similar to WsKlineServe, but it handles multiple symbols with it interval
func WsCombinedKlineServe(symbolIntervalPair map[string]string, handler WsKlineHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := getCombinedEndpoint()
	for symbol, interval := range symbolIntervalPair {
		endpoint += fmt.Sprintf("%s@kline_%s", strings.ToLower(symbol), interval) + "/"
	}
	endpoint = endpoint[:len(endpoint)-1]
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		j, err := newJSON(message)
		if err != nil {
			errHandler(err)
			return
		}

		stream := j.Get("stream").MustString()
		data := j.Get("data").MustMap()

		symbol := strings.Split(stream, "@")[0]

		jsonData, _ := json.Marshal(data)

		event := new(WsKlineEvent)
		err = json.Unmarshal(jsonData, event)
		if err != nil {
			errHandler(err)
			return
		}
		event.Symbol = strings.ToUpper(symbol)

		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsKlineServe serve websocket kline handler with a symbol and interval like 15m, 30s
func WsKlineServe(symbol string, interval string, handler WsKlineHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@kline_%s", getWsEndpoint(), strings.ToLower(symbol), interval)
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsKlineEvent)
		err := json.Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsKlineEvent define websocket kline event
type WsKlineEvent struct {
	Event  string  `json:"e"`
	Time   int64   `json:"E"`
	Symbol string  `json:"s"`
	Kline  WsKline `json:"k"`
}

// WsKline define websocket kline
type WsKline struct {
	StartTime            int64  `json:"t"`
	EndTime              int64  `json:"T"`
	Symbol               string `json:"s"`
	Interval             string `json:"i"`
	FirstTradeID         int64  `json:"f"`
	LastTradeID          int64  `json:"L"`
	Open                 string `json:"o"`
	Close                string `json:"c"`
	High                 string `json:"h"`
	Low                  string `json:"l"`
	Volume               string `json:"v"`
	TradeNum             int64  `json:"n"`
	IsFinal              bool   `json:"x"`
	QuoteVolume          string `json:"q"`
	ActiveBuyVolume      string `json:"V"`
	ActiveBuyQuoteVolume string `json:"Q"`
}

// WsAggTradeHandler handle websocket aggregate trade event
type WsAggTradeHandler func(event *WsAggTradeEvent)

// WsAggTradeServe serve websocket aggregate handler with a symbol
func WsAggTradeServe(symbol string, handler WsAggTradeHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@aggTrade", getWsEndpoint(), strings.ToLower(symbol))
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsAggTradeEvent)
		err := json.Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsCombinedAggTradeServe is similar to WsAggTradeServe, but it handles multiple symbolx
func WsCombinedAggTradeServe(symbols []string, handler WsAggTradeHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := getCombinedEndpoint()
	for s := range symbols {
		endpoint += fmt.Sprintf("%s@aggTrade", strings.ToLower(symbols[s])) + "/"
	}
	endpoint = endpoint[:len(endpoint)-1]
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		j, err := newJSON(message)
		if err != nil {
			errHandler(err)
			return
		}

		stream := j.Get("stream").MustString()
		data := j.Get("data").MustMap()

		symbol := strings.Split(stream, "@")[0]

		jsonData, _ := json.Marshal(data)

		event := new(WsAggTradeEvent)
		err = json.Unmarshal(jsonData, event)
		if err != nil {
			errHandler(err)
			return
		}

		event.Symbol = strings.ToUpper(symbol)

		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

type WsAssetIndexEvent struct {
	Event                 string `json:"e"`
	Symbol                string `json:"s"`
	Time                  int64  `json:"E"`
	Index                 string `json:"i"`
	BidBuffer             string `json:"b"`
	AskBuffer             string `json:"a"`
	BidRate               string `json:"B"`
	AskRate               string `json:"A"`
	AutoExchangeBidBuffer string `json:"q"`
	AutoExchangeAskBuffer string `json:"g"`
	AutoExchangeBidRate   string `json:"Q"`
	AutoExchangeAskRate   string `json:"G"`
}

type WsAssetIndexHandler func(event []WsAssetIndexEvent)

func WsAssetIndexServer(handler WsAssetIndexHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/!assetIndex@arr", getWsEndpoint())
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := []WsAssetIndexEvent{}
		err := json.Unmarshal(message, &event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsAggTradeEvent define websocket aggregate trade event
type WsAggTradeEvent struct {
	Event                 string `json:"e"`
	Time                  int64  `json:"E"`
	Symbol                string `json:"s"`
	AggTradeID            int64  `json:"a"`
	Price                 string `json:"p"`
	Quantity              string `json:"q"`
	FirstBreakdownTradeID int64  `json:"f"`
	LastBreakdownTradeID  int64  `json:"l"`
	TradeTime             int64  `json:"T"`
	IsBuyerMaker          bool   `json:"m"`
	Placeholder           bool   `json:"M"` // add this field to avoid case insensitive unmarshaling
}

// WsTradeHandler handle websocket trade event
type WsTradeHandler func(event *WsTradeEvent)
type WsCombinedTradeHandler func(event *WsCombinedTradeEvent)

// WsTradeServe serve websocket handler with a symbol
func WsTradeServe(symbol string, handler WsTradeHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@trade", getWsEndpoint(), strings.ToLower(symbol))
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsTradeEvent)
		err := json.Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

func WsCombinedTradeServe(symbols []string, handler WsCombinedTradeHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := getCombinedEndpoint()
	for _, s := range symbols {
		endpoint += fmt.Sprintf("%s@trade/", strings.ToLower(s))
	}
	endpoint = endpoint[:len(endpoint)-1]
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsCombinedTradeEvent)
		err := json.Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsTradeEvent define websocket trade event
type WsTradeEvent struct {
	Event         string `json:"e"`
	Time          int64  `json:"E"`
	Symbol        string `json:"s"`
	TradeID       int64  `json:"t"`
	Price         string `json:"p"`
	Quantity      string `json:"q"`
	BuyerOrderID  int64  `json:"b"`
	SellerOrderID int64  `json:"a"`
	TradeTime     int64  `json:"T"`
	IsBuyerMaker  bool   `json:"m"`
	Placeholder   bool   `json:"M"` // add this field to avoid case insensitive unmarshaling
}

type WsCombinedTradeEvent struct {
	Stream string       `json:"stream"`
	Data   WsTradeEvent `json:"data"`
}

// WsUserDataEvent define user data event
type WsUserDataEvent struct {
	Event               UserDataEventType      `json:"e"`
	Time                int64                  `json:"E"`
	TransactionTime     int64                  `json:"T"`
	AccountUpdateTime   int64                  `json:"u"`
	OrderUpdate         *WsOrderUpdate         `json:"o"`
	AccountUpdate       *WsAccountUpdateList   `json:"a"`
	AccountConfigUpdate *WSAccountConfigUpdate `json:"ac"`
}

type WSAccountConfigUpdate struct {
	Symbol   string `json:"s"`
	Leverage int    `json:"l"`
}

type WsAccountUpdateList struct {
	EventType string             `json:"m"`
	Balances  []WsBalanceUpdate  `json:"B"`
	Positions []WsPositionUpdate `json:"P"`
}

// WsAccountUpdate define account update

type WsPositionUpdate struct {
	Symbol              string `json:"s"`
	PositionAmount      string `json:"pa"`
	EntryPrice          string `json:"ep"`
	AccumulatedRealized string `json:"cr"`
	UnrealizedPL        string `json:"up"`
	MarginType          string `json:"mt"`
	IsolatedWallet      string `json:"iw"`
	PositionSide        string `json:"ps"`
}

type WsBalanceUpdate struct {
	Asset              string `json:"a"`
	WalletBalance      string `json:"wb"`
	CrossWalletBalance string `json:"cw"`
	BalanceChange      string `json:"bc"`
}

type WsOrderUpdate struct {
	Id              int64           `json:"i"` // order id
	Symbol          string          `json:"s"`
	ClientOrderId   string          `json:"c"`
	Side            string          `json:"S"`
	Type            string          `json:"o"`
	TimeInForce     TimeInForceType `json:"f"`
	Volume          string          `json:"q"`
	OrgPrice        string          `json:"p"`
	ArgPrice        string          `json:"ap"`
	StopPrice       string          `json:"sp"`
	ExecutionType   string          `json:"x"` // execution type for this event NEW/TRADE...
	Status          string          `json:"X"` // order status
	LatestVolume    string          `json:"l"` // quantity for the latest trade (latest filled)
	FilledVolume    string          `json:"z"`
	LatestPrice     string          `json:"L"` // price for the latest trade
	FeeAsset        string          `json:"N"`
	FeeCost         string          `json:"n"`
	TransactionTime int64           `json:"T"`
	TradeId         int64           `json:"t"`
	BidNotional     string          `json:"b"`
	AskNotional     string          `json:"a"`
	IsMaker         bool            `json:"m"` // is this order maker?
	IsReduceOnly    bool            `json:"R"`
	OrgOrderType    string          `json:"ot"`
	PositionSide    string          `json:"ps"`
	ActivationPrice string          `json:"AP"`
	RealizedProfit  string          `json:"rp"`
}

type WsOCOUpdate struct {
	Symbol          string `json:"s"`
	OrderListId     int64  `json:"g"`
	ContingencyType string `json:"c"`
	ListStatusType  string `json:"l"`
	ListOrderStatus string `json:"L"`
	RejectReason    string `json:"r"`
	ClientOrderId   string `json:"C"` // List Client Order ID
	Orders          WsOCOOrderList
}

type WsOCOOrderList struct {
	WsOCOOrders []WsOCOOrder `json:"O"`
}

type WsOCOOrder struct {
	Symbol        string `json:"s"`
	OrderId       int64  `json:"i"`
	ClientOrderId string `json:"c"`
}

// WsUserDataHandler handle WsUserDataEvent
type WsUserDataHandler func(event *WsUserDataEvent)

// WsUserDataServe serve user data handler with listen key
func WsUserDataServe(listenKey string, handler WsUserDataHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s", getWsEndpoint(), listenKey)
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {

		event := new(WsUserDataEvent)
		err = json.Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}

		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsMarketStatHandler handle websocket that push single market statistics for 24hr
type WsMarketStatHandler func(event *WsMarketStatEvent)

// WsCombinedMarketStatServe is similar to WsMarketStatServe, but it handles multiple symbolx
func WsCombinedMarketStatServe(symbols []string, handler WsMarketStatHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := getCombinedEndpoint()
	for s := range symbols {
		endpoint += fmt.Sprintf("%s@ticker", strings.ToLower(symbols[s])) + "/"
	}
	endpoint = endpoint[:len(endpoint)-1]
	cfg := newWsConfig(endpoint)

	wsHandler := func(message []byte) {
		j, err := newJSON(message)
		if err != nil {
			errHandler(err)
			return
		}

		stream := j.Get("stream").MustString()
		data := j.Get("data").MustMap()

		symbol := strings.Split(stream, "@")[0]

		jsonData, _ := json.Marshal(data)

		event := new(WsMarketStatEvent)
		err = json.Unmarshal(jsonData, event)
		if err != nil {
			errHandler(err)
			return
		}

		event.Symbol = strings.ToUpper(symbol)

		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsMarketStatServe serve websocket that push 24hr statistics for single market every second
func WsMarketStatServe(symbol string, handler WsMarketStatHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@ticker", getWsEndpoint(), strings.ToLower(symbol))
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		var event WsMarketStatEvent
		err := json.Unmarshal(message, &event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(&event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsAllMarketsStatHandler handle websocket that push all markets statistics for 24hr
type WsAllMarketsStatHandler func(event WsAllMarketsStatEvent)

// WsAllMarketsStatServe serve websocket that push 24hr statistics for all market every second
func WsAllMarketsStatServe(handler WsAllMarketsStatHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/!ticker@arr", getWsEndpoint())
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		var event WsAllMarketsStatEvent
		err := json.Unmarshal(message, &event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsAllMarketsStatEvent define array of websocket market statistics events
type WsAllMarketsStatEvent []*WsMarketStatEvent

// WsMarketStatEvent define websocket market statistics event
type WsMarketStatEvent struct {
	Event              string `json:"e"`
	Time               int64  `json:"E"`
	Symbol             string `json:"s"`
	PriceChange        string `json:"p"`
	PriceChangePercent string `json:"P"`
	WeightedAvgPrice   string `json:"w"`
	PrevClosePrice     string `json:"x"`
	LastPrice          string `json:"c"`
	CloseQty           string `json:"Q"`
	BidPrice           string `json:"b"`
	BidQty             string `json:"B"`
	AskPrice           string `json:"a"`
	AskQty             string `json:"A"`
	OpenPrice          string `json:"o"`
	HighPrice          string `json:"h"`
	LowPrice           string `json:"l"`
	BaseVolume         string `json:"v"`
	QuoteVolume        string `json:"q"`
	OpenTime           int64  `json:"O"`
	CloseTime          int64  `json:"C"`
	FirstID            int64  `json:"F"`
	LastID             int64  `json:"L"`
	Count              int64  `json:"n"`
}

// WsAllMiniMarketsStatServeHandler handle websocket that push all mini-ticker market statistics for 24hr
type WsAllMiniMarketsStatServeHandler func(event WsAllMiniMarketsStatEvent)

// WsAllMiniMarketsStatServe serve websocket that push mini version of 24hr statistics for all market every second
func WsAllMiniMarketsStatServe(handler WsAllMiniMarketsStatServeHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/!miniTicker@arr", getWsEndpoint())
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		var event WsAllMiniMarketsStatEvent
		err := json.Unmarshal(message, &event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsAllMiniMarketsStatEvent define array of websocket market mini-ticker statistics events
type WsAllMiniMarketsStatEvent []*WsMiniMarketsStatEvent

// WsMiniMarketsStatEvent define websocket market mini-ticker statistics event
type WsMiniMarketsStatEvent struct {
	Event       string `json:"e"`
	Time        int64  `json:"E"`
	Symbol      string `json:"s"`
	LastPrice   string `json:"c"`
	OpenPrice   string `json:"o"`
	HighPrice   string `json:"h"`
	LowPrice    string `json:"l"`
	BaseVolume  string `json:"v"`
	QuoteVolume string `json:"q"`
}

// WsBookTickerEvent define websocket best book ticker event.
type WsBookTickerEvent struct {
	UpdateID     int64  `json:"u"`
	Symbol       string `json:"s"`
	BestBidPrice string `json:"b"`
	BestBidQty   string `json:"B"`
	BestAskPrice string `json:"a"`
	BestAskQty   string `json:"A"`
}

type WsCombinedBookTickerEvent struct {
	Data   *WsBookTickerEvent `json:"data"`
	Stream string             `json:"stream"`
}

// WsBookTickerHandler handle websocket that pushes updates to the best bid or ask price or quantity in real-time for a specified symbol.
type WsBookTickerHandler func(event *WsBookTickerEvent)

// WsBookTickerServe serve websocket that pushes updates to the best bid or ask price or quantity in real-time for a specified symbol.
func WsBookTickerServe(symbol string, handler WsBookTickerHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@bookTicker", getWsEndpoint(), strings.ToLower(symbol))
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsBookTickerEvent)
		err := json.Unmarshal(message, &event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsCombinedBookTickerServe is similar to WsBookTickerServe, but it is for multiple symbols
func WsCombinedBookTickerServe(symbols []string, handler WsBookTickerHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := baseCombinedMainURL
	for _, s := range symbols {
		endpoint += fmt.Sprintf("%s@bookTicker", strings.ToLower(s)) + "/"
	}
	endpoint = endpoint[:len(endpoint)-1]
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsCombinedBookTickerEvent)
		err := json.Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event.Data)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsAllBookTickerServe serve websocket that pushes updates to the best bid or ask price or quantity in real-time for all symbols.
func WsAllBookTickerServe(handler WsBookTickerHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/!bookTicker", getWsEndpoint())
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsBookTickerEvent)
		err := json.Unmarshal(message, &event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsMarkPriceEvent define websocket markPriceUpdate event.
type WsMarkPriceEvent struct {
	Event                string `json:"e"`
	Time                 int64  `json:"E"`
	Symbol               string `json:"s"`
	MarkPrice            string `json:"p"`
	IndexPrice           string `json:"i"`
	EstimatedSettlePrice string `json:"P"`
	FundingRate          string `json:"r"`
	NextFundingTime      int64  `json:"T"`
}

// WsMarkPriceForAllEvent defines an array of websocket markPriceUpdate events.
type WsMarkPriceForAllEvent []*WsMarkPriceEvent

type WsCombinedMarkPriceForAllEvent struct {
	Data   *WsMarkPriceForAllEvent `json:"data"`
	Stream string                  `json:"stream"`
}

// WsMarkPriceHandler handle websocket that pushes updates to the markPrice for all symbol.
type WsMarkPriceForAllHandler func(event *WsMarkPriceForAllEvent)

// WsCombinedMarkPriceForAllServe websocket that pushes mark price multiple symbol.
func WsCombinedMarkPriceForAllServe(handler WsMarkPriceForAllHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s!markPrice@arr", getCombinedEndpoint())
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		j, err := newJSON(message)
		if err != nil {
			errHandler(err)
			return
		}

		data := j.Get("data")
		jsonData, _ := json.Marshal(data)

		event := new(WsMarkPriceForAllEvent)
		err = json.Unmarshal(jsonData, event)
		if err != nil {
			errHandler(err)
			return
		}

		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}
