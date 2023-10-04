package futures

import (
	"context"
	"net/http"
)

// DepthResponse define depth info with bids and asks
type AssetIndexResponse struct {
	Symbol                string `json:"symbol"`
	Time                  int64  `json:"time"`
	Index                 string `json:"index"`
	BidBuffer             string `json:"bidBuffer"`
	AskBuffer             string `json:"askBuffer"`
	BidRate               string `json:"bidRate"`
	AskRate               string `json:"askRate"`
	AutoExchangeBidBuffer string `json:"autoExchangeBidBuffer"`
	AutoExchangeAskBuffer string `json:"autoExchangeAskBuffer"`
	AutoExchangeBidRate   string `json:"autoExchangeBidRate"`
	AutoExchangeAskRate   string `json:"autoExchangeAskRate"`
}

// DepthService show depth info
type AssetIndexService struct {
	c *Client
}

// Do send request
func (s *AssetIndexService) Do(ctx context.Context, opts ...RequestOption) (res []AssetIndexResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/assetIndex",
	}

	data, _, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	j, err := newJSON(data)
	if err != nil {
		return nil, err
	}

	res = []AssetIndexResponse{}
	for i := range j.MustArray() {
		idx := j.GetIndex(i)
		res = append(res, AssetIndexResponse{
			Symbol:                idx.Get("symbol").MustString(),
			Time:                  idx.Get("time").MustInt64(),
			Index:                 idx.Get("index").MustString(),
			BidBuffer:             idx.Get("bidBuffer").MustString(),
			AskBuffer:             idx.Get("askBuffer").MustString(),
			BidRate:               idx.Get("bidRate").MustString(),
			AskRate:               idx.Get("askRate").MustString(),
			AutoExchangeBidBuffer: idx.Get("autoExchangeBidBuffer").MustString(),
			AutoExchangeAskBuffer: idx.Get("autoExchangeAskBuffer").MustString(),
			AutoExchangeBidRate:   idx.Get("autoExchangeBidRate").MustString(),
			AutoExchangeAskRate:   idx.Get("autoExchangeAskRate").MustString(),
		})
	}

	return res, nil
}
