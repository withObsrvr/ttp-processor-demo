package txview

import "sort"

func BuildSemanticResponse(info TransactionInfo, ops []Operation, events []Event, summary TxSummary) SemanticTransactionResponse {
	actors := BuildBasicActors(info, ops, events)
	return SemanticTransactionResponse{
		Transaction:    info,
		Classification: ClassifyTransaction(summary, ops, actors),
		Actors:         actors,
		Assets:         DeriveAssetContext(summary, ops, events),
		Operations:     ops,
		Events:         events,
		LegacySummary:  summary,
	}
}

func BuildBasicActors(info TransactionInfo, ops []Operation, events []Event) []SemanticActor {
	type actorState struct {
		actor      SemanticActor
		roles      map[string]struct{}
		contractID *string
	}
	actors := map[string]*actorState{}
	add := func(actorID, actorType, role string, contractID *string) {
		if actorID == "" {
			return
		}
		st, ok := actors[actorID]
		if !ok {
			st = &actorState{actor: SemanticActor{ActorID: actorID, ActorType: actorType}, roles: map[string]struct{}{}}
			actors[actorID] = st
		}
		if actorType != "" {
			st.actor.ActorType = actorType
		}
		if contractID != nil {
			st.actor.ContractID = contractID
		}
		if role != "" {
			st.roles[role] = struct{}{}
		}
	}

	submitter := ""
	if info.SourceAccount != nil {
		submitter = *info.SourceAccount
	}
	if submitter == "" && len(ops) > 0 {
		submitter = ops[0].SourceAccount
	}
	if submitter != "" {
		add(submitter, actorTypeForID(submitter), "submitter", nil)
		add(submitter, actorTypeForID(submitter), "fee_payer", nil)
	}

	for _, op := range ops {
		if op.SourceAccount != "" {
			add(op.SourceAccount, actorTypeForID(op.SourceAccount), "counterparty", nil)
			if op.IsSorobanOp {
				add(op.SourceAccount, actorTypeForID(op.SourceAccount), "invoker", nil)
			} else {
				add(op.SourceAccount, actorTypeForID(op.SourceAccount), "effective_actor", nil)
			}
		}
		if op.Destination != nil {
			add(*op.Destination, actorTypeForID(*op.Destination), "receiver", nil)
			add(*op.Destination, actorTypeForID(*op.Destination), "beneficiary", nil)
		}
		if op.ContractID != nil {
			add(*op.ContractID, actorTypeForID(*op.ContractID), "protocol", op.ContractID)
		}
	}

	for _, event := range events {
		if event.From != nil {
			add(*event.From, actorTypeForID(*event.From), "sender", nil)
			add(*event.From, actorTypeForID(*event.From), "asset_owner", nil)
			add(*event.From, actorTypeForID(*event.From), "effective_actor", nil)
		}
		if event.To != nil {
			add(*event.To, actorTypeForID(*event.To), "receiver", nil)
			add(*event.To, actorTypeForID(*event.To), "beneficiary", nil)
		}
		if event.ContractID != nil {
			add(*event.ContractID, actorTypeForID(*event.ContractID), "protocol", event.ContractID)
		}
	}

	if submitter != "" {
		if st, ok := actors[submitter]; ok {
			if _, has := st.roles["effective_actor"]; !has {
				st.roles["effective_actor"] = struct{}{}
			}
		}
	}

	out := make([]SemanticActor, 0, len(actors))
	for _, st := range actors {
		roles := make([]string, 0, len(st.roles))
		for role := range st.roles {
			roles = append(roles, role)
		}
		sort.Strings(roles)
		st.actor.Roles = roles
		out = append(out, st.actor)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ActorID < out[j].ActorID })
	return out
}

func ClassifyTransaction(summary TxSummary, ops []Operation, actors []SemanticActor) SemanticTransactionClassification {
	opTypes := make([]string, 0, len(ops))
	seen := map[string]struct{}{}
	for _, op := range ops {
		if op.TypeName == "" {
			continue
		}
		if _, ok := seen[op.TypeName]; ok {
			continue
		}
		seen[op.TypeName] = struct{}{}
		opTypes = append(opTypes, op.TypeName)
	}
	sort.Strings(opTypes)

	txType := summary.Type
	subtype := ""
	switch summary.Type {
	case "change_trust":
		txType = "trustline_change"
	case "contract_call":
		if len(ops) == 1 && ops[0].FunctionName != nil {
			subtype = *ops[0].FunctionName
		}
	}
	if txType == "" || txType == "unknown" {
		if len(opTypes) > 0 {
			txType = opTypes[0]
		} else {
			txType = "unknown"
		}
	}

	effectiveActorType := ""
	walletInvolved := false
	for _, actor := range actors {
		if actor.ActorType == "smart_wallet" {
			walletInvolved = true
		}
		for _, role := range actor.Roles {
			if role == "effective_actor" && effectiveActorType == "" {
				effectiveActorType = actor.ActorType
			}
		}
	}
	if effectiveActorType == "" && len(actors) > 0 {
		effectiveActorType = actors[0].ActorType
	}

	confidence := "high"
	if txType == "unknown" {
		confidence = "low"
	}
	return SemanticTransactionClassification{
		TxType:             txType,
		Subtype:            subtype,
		Confidence:         confidence,
		OperationTypes:     opTypes,
		WalletInvolved:     walletInvolved,
		EffectiveActorType: effectiveActorType,
	}
}

func DeriveAssetContext(summary TxSummary, ops []Operation, events []Event) SemanticAssetContext {
	ctx := SemanticAssetContext{}
	movements := make([]SemanticAssetMovement, 0, len(events))
	for _, e := range events {
		movement := SemanticAssetMovement{
			Amount:   deref(e.Amount),
			Asset:    eventAsset(e),
			From:     e.From,
			To:       e.To,
			Contract: e.ContractID,
			Kind:     e.EventType,
		}
		movements = append(movements, movement)
	}
	if len(movements) == 0 {
		switch {
		case summary.Transfer != nil:
			movements = append(movements, SemanticAssetMovement{Amount: summary.Transfer.Amount, Asset: summary.Transfer.Asset, Kind: "transfer", From: stringPtrOrNil(summary.Transfer.From), To: stringPtrOrNil(summary.Transfer.To)})
		case summary.Mint != nil:
			movements = append(movements, SemanticAssetMovement{Amount: summary.Mint.Amount, Asset: summary.Mint.Asset, Kind: "mint", To: stringPtrOrNil(summary.Mint.To)})
		case summary.Burn != nil:
			movements = append(movements, SemanticAssetMovement{Amount: summary.Burn.Amount, Asset: summary.Burn.Asset, Kind: "burn", From: stringPtrOrNil(summary.Burn.From)})
		}
	}
	ctx.Movements = movements
	for i := range movements {
		m := movements[i]
		switch m.Kind {
		case "burn":
			if ctx.Sent == nil {
				mv := m
				ctx.Sent = &mv
			}
		case "mint":
			if ctx.Received == nil {
				mv := m
				ctx.Received = &mv
			}
		default:
			if ctx.Sent == nil {
				mv := m
				ctx.Sent = &mv
			}
			if ctx.Received == nil {
				mv := m
				ctx.Received = &mv
			}
		}
	}
	if summary.Swap != nil {
		ctx.Path = []string{summary.Swap.SoldAsset, summary.Swap.BoughtAsset}
	}
	return ctx
}

func actorTypeForID(actorID string) string {
	if actorID == "" {
		return "unknown_actor"
	}
	switch actorID[0] {
	case 'G':
		return "classic_account"
	case 'C':
		return "contract"
	default:
		return "unknown_actor"
	}
}

func eventAsset(e Event) string {
	if e.AssetCode != nil && *e.AssetCode != "" {
		if e.AssetIssuer != nil && *e.AssetIssuer != "" {
			return *e.AssetCode + ":" + *e.AssetIssuer
		}
		return *e.AssetCode
	}
	if e.ContractID != nil && *e.ContractID != "" {
		return *e.ContractID
	}
	return "asset"
}

func deref(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func stringPtrOrNil(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}
