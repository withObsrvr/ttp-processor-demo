package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (h *SilverHotReader) GetOHLCCandles(ctx context.Context, baseCode, baseIssuer, counterCode, counterIssuer, interval string, startTime, endTime time.Time) ([]OHLCCandle, error) {
	bucketExpr := "date_trunc('hour', trade_timestamp)"
	switch interval {
	case "1m":
		bucketExpr = "to_timestamp(floor(extract(epoch from trade_timestamp) / 60) * 60)"
	case "5m":
		bucketExpr = "to_timestamp(floor(extract(epoch from trade_timestamp) / 300) * 300)"
	case "15m":
		bucketExpr = "to_timestamp(floor(extract(epoch from trade_timestamp) / 900) * 900)"
	case "1h":
		bucketExpr = "date_trunc('hour', trade_timestamp)"
	case "4h":
		bucketExpr = "to_timestamp(floor(extract(epoch from trade_timestamp) / 14400) * 14400)"
	case "1d":
		bucketExpr = "date_trunc('day', trade_timestamp)"
	case "1w":
		bucketExpr = "date_trunc('week', trade_timestamp)"
	}

	query := fmt.Sprintf(`
		WITH filtered AS (
			SELECT %s as bucket, trade_timestamp, price::double precision as price, selling_amount::double precision as selling_amount
			FROM trades
			WHERE (($1 = 'XLM' AND (selling_asset_code IS NULL OR selling_asset_code = '')) OR selling_asset_code = $1)
			  AND (($2 = 'XLM' AND (buying_asset_code IS NULL OR buying_asset_code = '')) OR buying_asset_code = $2)
			  AND ($3::text IS NULL OR $3 = '' OR selling_asset_issuer = $3)
			  AND ($4::text IS NULL OR $4 = '' OR buying_asset_issuer = $4)
			  AND trade_timestamp >= $5 AND trade_timestamp <= $6
		)
		SELECT
			bucket,
			(ARRAY_AGG(price ORDER BY trade_timestamp ASC))[1] as open_price,
			MAX(price) as high_price,
			MIN(price) as low_price,
			(ARRAY_AGG(price ORDER BY trade_timestamp DESC))[1] as close_price,
			SUM(selling_amount) as volume,
			COUNT(*) as trade_count
		FROM filtered
		GROUP BY bucket
		ORDER BY bucket ASC
		LIMIT 500
	`, bucketExpr)

	rows, err := h.db.QueryContext(ctx, query, baseCode, counterCode, baseIssuer, counterIssuer, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("GetOHLCCandles: %w", err)
	}
	defer rows.Close()

	var candles []OHLCCandle
	for rows.Next() {
		var c OHLCCandle
		var ts time.Time
		if err := rows.Scan(&ts, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume, &c.TradeCount); err != nil {
			return nil, err
		}
		c.Timestamp = ts.Format(time.RFC3339)
		candles = append(candles, c)
	}
	return candles, nil
}

func (h *SilverHotReader) GetLatestPrice(ctx context.Context, baseCode, baseIssuer, counterCode, counterIssuer string) (*LatestPrice, error) {
	query := `
		WITH latest AS (
			SELECT price::double precision as price, trade_timestamp
			FROM trades
			WHERE (($1 = 'XLM' AND (selling_asset_code IS NULL OR selling_asset_code = '')) OR selling_asset_code = $1)
			  AND (($2 = 'XLM' AND (buying_asset_code IS NULL OR buying_asset_code = '')) OR buying_asset_code = $2)
			  AND ($3::text IS NULL OR $3 = '' OR selling_asset_issuer = $3)
			  AND ($4::text IS NULL OR $4 = '' OR buying_asset_issuer = $4)
			ORDER BY trade_timestamp DESC
			LIMIT 1
		),
		stats_24h AS (
			SELECT SUM(selling_amount::double precision) as vol, COUNT(*) as cnt
			FROM trades
			WHERE (($1 = 'XLM' AND (selling_asset_code IS NULL OR selling_asset_code = '')) OR selling_asset_code = $1)
			  AND (($2 = 'XLM' AND (buying_asset_code IS NULL OR buying_asset_code = '')) OR buying_asset_code = $2)
			  AND ($3::text IS NULL OR $3 = '' OR selling_asset_issuer = $3)
			  AND ($4::text IS NULL OR $4 = '' OR buying_asset_issuer = $4)
			  AND trade_timestamp >= NOW() - INTERVAL '24 hours'
		)
		SELECT l.price, l.trade_timestamp, COALESCE(s.vol, 0), COALESCE(s.cnt, 0)
		FROM latest l, stats_24h s
	`

	var lp LatestPrice
	var ts time.Time
	err := h.db.QueryRowContext(ctx, query, baseCode, counterCode, baseIssuer, counterIssuer).Scan(&lp.Price, &ts, &lp.Volume24h, &lp.TradeCount24h)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("no trades found for pair %s/%s", baseCode, counterCode)
	}
	if err != nil {
		return nil, fmt.Errorf("GetLatestPrice: %w", err)
	}
	lp.BaseAsset = baseCode
	lp.CounterAsset = counterCode
	lp.Timestamp = ts.Format(time.RFC3339)
	return &lp, nil
}

func (h *SilverHotReader) GetTradePairs(ctx context.Context) ([]TradePair, error) {
	rows, err := h.db.QueryContext(ctx, `
		SELECT COALESCE(NULLIF(selling_asset_code, ''), 'XLM') as selling_asset_code,
		       selling_asset_issuer,
		       COALESCE(NULLIF(buying_asset_code, ''), 'XLM') as buying_asset_code,
		       buying_asset_issuer,
		       COUNT(*) as trade_count
		FROM trades
		GROUP BY selling_asset_code, selling_asset_issuer, buying_asset_code, buying_asset_issuer
		HAVING COUNT(*) >= 5
		ORDER BY trade_count DESC
		LIMIT 100
	`)
	if err != nil {
		return nil, fmt.Errorf("GetTradePairs: %w", err)
	}
	defer rows.Close()

	var pairs []TradePair
	for rows.Next() {
		var p TradePair
		var sellingCode, sellingIssuer, buyingCode, buyingIssuer sql.NullString
		if err := rows.Scan(&sellingCode, &sellingIssuer, &buyingCode, &buyingIssuer, &p.TradeCount); err != nil {
			return nil, err
		}
		if sellingCode.Valid {
			p.BaseAssetCode = sellingCode.String
		}
		if sellingIssuer.Valid {
			p.BaseAssetIssuer = sellingIssuer.String
		}
		if buyingCode.Valid {
			p.CounterAssetCode = buyingCode.String
		}
		if buyingIssuer.Valid {
			p.CounterAssetIssuer = buyingIssuer.String
		}
		pairs = append(pairs, p)
	}
	return pairs, nil
}
