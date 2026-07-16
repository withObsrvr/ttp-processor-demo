package main

import (
	"context"
	"fmt"
)

type HorizonEffectReader struct {
	fallback horizonEffectReader
	serving  *SilverHotReader
}

func NewHorizonEffectReader(fallback horizonEffectReader, serving *SilverHotReader) *HorizonEffectReader {
	if fallback == nil && serving == nil {
		return nil
	}
	return &HorizonEffectReader{fallback: fallback, serving: serving}
}

func (r *HorizonEffectReader) GetEffects(ctx context.Context, filters EffectFilters) ([]SilverEffect, string, bool, error) {
	if r == nil {
		return nil, "", false, fmt.Errorf("horizon effect reader unavailable")
	}
	if r.serving != nil {
		effects, next, hasMore, covered, err := r.serving.GetServingOperationEffects(ctx, filters)
		if err != nil {
			return nil, "", false, err
		}
		if covered {
			return effects, next, hasMore, nil
		}
		effects, next, hasMore, covered, err = r.serving.GetServingTransactionEffects(ctx, filters)
		if err != nil {
			return nil, "", false, err
		}
		if covered {
			return effects, next, hasMore, nil
		}
		effects, next, hasMore, covered, err = r.serving.GetServingAccountEffects(ctx, filters)
		if err != nil {
			return nil, "", false, err
		}
		if covered {
			return effects, next, hasMore, nil
		}
	}
	if r.fallback == nil {
		return nil, "", false, fmt.Errorf("horizon effect reader unavailable")
	}
	return r.fallback.GetEffects(ctx, filters)
}
