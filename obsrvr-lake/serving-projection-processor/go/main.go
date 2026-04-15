package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func buildProjectors(cfg *Config, bronze, silver, serving *pgxpool.Pool, checkpoints *CheckpointStore) []ProjectorRunner {

	var projectors []ProjectorRunner
	if cfg.Projectors.LedgersRecent.Enabled {
		projectors = append(projectors, NewLedgersRecentProjector("testnet", cfg.Projectors.LedgersRecent.BatchSize, bronze, serving, checkpoints))
	}
	if cfg.Projectors.TransactionsRecent.Enabled {
		projectors = append(projectors, NewTransactionsRecentProjector("testnet", cfg.Projectors.TransactionsRecent.BatchSize, bronze, silver, serving, checkpoints))
	}
	if cfg.Projectors.AccountsCurrent.Enabled {
		projectors = append(projectors, NewAccountsCurrentProjector("testnet", cfg.Projectors.AccountsCurrent.BatchSize, silver, serving, checkpoints))
	}
	if cfg.Projectors.AccountBalances.Enabled {
		projectors = append(projectors, NewAccountBalancesProjector("testnet", cfg.Projectors.AccountBalances.BatchSize, silver, serving, checkpoints))
	}
	if cfg.Projectors.NetworkStats.Enabled {
		projectors = append(projectors, NewNetworkStatsProjector("testnet", serving))
	}
	if cfg.Projectors.AssetStats.Enabled {
		projectors = append(projectors, NewAssetStatsProjector("testnet", serving))
	}
	if cfg.Projectors.ContractsCurrent.Enabled {
		projectors = append(projectors, NewContractsCurrentProjector("testnet", silver, serving))
	}
	if cfg.Projectors.ContractStats.Enabled {
		projectors = append(projectors, NewContractStatsProjector("testnet", silver, serving))
	}
	if cfg.Projectors.OperationsRecent.Enabled {
		projectors = append(projectors, NewOperationsRecentProjector("testnet", cfg.Projectors.OperationsRecent.BatchSize, silver, serving, checkpoints))
	}
	if cfg.Projectors.EventsRecent.Enabled {
		projectors = append(projectors, NewEventsRecentProjector("testnet", cfg.Projectors.EventsRecent.BatchSize, bronze, serving, checkpoints))
	}
	if cfg.Projectors.ExplorerEventsRecent.Enabled {
		projectors = append(projectors, NewExplorerEventsRecentProjector("testnet", cfg.Projectors.ExplorerEventsRecent.BatchSize, bronze, silver, serving, checkpoints))
	}
	if cfg.Projectors.ContractCallsRecent.Enabled {
		projectors = append(projectors, NewContractCallsRecentProjector("testnet", cfg.Projectors.ContractCallsRecent.BatchSize, silver, serving, checkpoints))
	}
	return projectors
}

func main() {
	configPath := flag.String("config", "config.yaml", "Path to config file")
	applySchemaOnly := flag.Bool("apply-schema-only", false, "Apply serving schema and exit")
	flag.Parse()

	cfg, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	bronzePool, err := openPool(ctx, cfg.Source.BronzeHot)
	if err != nil {
		log.Fatalf("connect bronze_hot: %v", err)
	}
	defer bronzePool.Close()

	silverPool, err := openPool(ctx, cfg.Source.SilverHot)
	if err != nil {
		log.Fatalf("connect silver_hot: %v", err)
	}
	defer silverPool.Close()

	servingPool, err := openPool(ctx, cfg.Target.ServingPostgres)
	if err != nil {
		log.Fatalf("connect serving postgres: %v", err)
	}
	defer servingPool.Close()

	if cfg.Schema.AutoApply || *applySchemaOnly {
		log.Println("applying serving schema...")
		if err := EnsureServingSchema(ctx, servingPool); err != nil {
			log.Fatalf("ensure serving schema: %v", err)
		}
		log.Println("serving schema ready")
		if *applySchemaOnly {
			return
		}
	}

	checkpoints := NewCheckpointStore(servingPool)
	healthServer := NewHealthServer(cfg.Service.Name, cfg.Health.Port, cfg.FallbackPollInterval())
	if err := healthServer.Start(); err != nil {
		log.Fatalf("start health server: %v", err)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := healthServer.Stop(shutdownCtx); err != nil {
			log.Printf("stop health server: %v", err)
		}
	}()

	projectors := buildProjectors(cfg, bronzePool, silverPool, servingPool, checkpoints)
	if len(projectors) == 0 {
		log.Println("no projectors enabled; exiting")
		return
	}
	for _, p := range projectors {
		healthServer.RegisterProjector(p.Name())
	}
	if err := RunProjectors(ctx, healthServer, projectors); err != nil {
		log.Printf("initial run error: %v", err)
	}

	if cfg.Trigger.Mode == "grpc" {
		runGRPCTriggered(ctx, cfg, healthServer, projectors)
		return
	}
	runPolling(ctx, cfg, healthServer, projectors)
}

func runPolling(ctx context.Context, cfg *Config, healthServer *HealthServer, projectors []ProjectorRunner) {
	ticker := time.NewTicker(cfg.TickInterval())
	defer ticker.Stop()
	log.Printf("%s started; trigger_mode=poll tick_interval=%s", cfg.Service.Name, cfg.TickInterval())
	for {
		select {
		case <-ctx.Done():
			log.Println("shutdown requested")
			return
		case <-ticker.C:
			_ = RunProjectors(ctx, healthServer, projectors)
		}
	}
}

func runGRPCTriggered(ctx context.Context, cfg *Config, healthServer *HealthServer, projectors []ProjectorRunner) {
	client, err := NewSilverStreamClient(cfg.Trigger.Endpoint)
	if err != nil {
		log.Fatalf("create silver stream client: %v", err)
	}
	defer client.Close()

	startLedger := highestCheckpoint(healthServer, projectors)
	eventCh := client.StreamCheckpointEvents(ctx, startLedger)
	fallbackTicker := time.NewTicker(cfg.FallbackPollInterval())
	defer fallbackTicker.Stop()
	log.Printf("%s started; trigger_mode=grpc endpoint=%s fallback_poll=%s", cfg.Service.Name, cfg.Trigger.Endpoint, cfg.FallbackPollInterval())

	var pending bool
	var cycleRunning bool
	var latestTarget int64
	cycleDone := make(chan struct{}, 1)

	launchRun := func(reason string) {
		cycleRunning = true
		pending = false
		go func() {
			log.Printf("triggered projector run reason=%s target_ledger=%d", reason, latestTarget)
			_ = RunProjectors(ctx, healthServer, projectors)
			select {
			case cycleDone <- struct{}{}:
			case <-ctx.Done():
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			log.Println("shutdown requested")
			return
		case evt, ok := <-eventCh:
			if !ok {
				log.Println("silver trigger stream closed; exiting")
				return
			}
			if int64(evt.EndLedger) > latestTarget {
				latestTarget = int64(evt.EndLedger)
			}
			if cycleRunning {
				pending = true
				continue
			}
			launchRun("grpc")
		case <-fallbackTicker.C:
			if cycleRunning {
				pending = true
				continue
			}
			launchRun("fallback_poll")
		case <-cycleDone:
			cycleRunning = false
			if pending || anyProjectorBehind(healthServer, projectors, latestTarget) {
				launchRun("coalesced")
			}
		}
	}
}
