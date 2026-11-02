package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"os"
	"time"

	"github.com/rs/zerolog"

	"github.com/evmi-cloud/go-evm-indexer/internal/autoloader"
	internal_bus "github.com/evmi-cloud/go-evm-indexer/internal/bus"
	"github.com/evmi-cloud/go-evm-indexer/internal/chaincheck"
	evmi_database "github.com/evmi-cloud/go-evm-indexer/internal/database/evmi-database"
	"github.com/evmi-cloud/go-evm-indexer/internal/exporter"
	"github.com/evmi-cloud/go-evm-indexer/internal/gateway"
	"github.com/evmi-cloud/go-evm-indexer/internal/grpc"
	"github.com/evmi-cloud/go-evm-indexer/internal/indexer"
	"github.com/evmi-cloud/go-evm-indexer/internal/metrics"
	"github.com/evmi-cloud/go-evm-indexer/internal/types"
	"github.com/urfave/cli/v2"
)

func main() {

	app := &cli.App{
		Name:        "EVMI",
		Description: "EVM Indexer",
		Commands: []*cli.Command{
			{
				Name:  "start",
				Usage: "Start EVM indexer",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "instance",
						Aliases: []string{"i"},
						EnvVars: []string{"EVMI_INSTANCE_ID"},
						Value:   "EVMI_INSTANCE_1",
						Usage:   "Database location location",
					},
					&cli.StringFlag{
						Name:    "config",
						Aliases: []string{"c"},
						EnvVars: []string{"CONFIG_FILE_PATH"},
						Value:   "/tmp/evm-indexer/config.json",
						Usage:   "Database location location",
					},
				},
				Action: func(cCtx *cli.Context) error {

					logger := zerolog.New(
						zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339},
					).Level(zerolog.TraceLevel).With().Timestamp().Caller().Logger()

					configPath := cCtx.String("config")
					instanceId := cCtx.String("instance")

					configFile, err := os.ReadFile(configPath)
					if err != nil {
						logger.Fatal().Msg(err.Error())
					}

					var config types.Config
					err = json.Unmarshal(configFile, &config)
					if err != nil {
						logger.Fatal().Msg(err.Error())
					}

					logger.Info().Msg("Verify the chain id of every configured node")
					if err := chaincheck.Verify(context.Background(), config.Resources.Blockchains); err != nil {
						logger.Fatal().Msg(err.Error())
					}
					logger.Info().Msg("Initialize Bus")
					internalBus := internal_bus.InitializeBus()

					logger.Info().Msg("Initialize Metrics")
					metrics := metrics.NewMetricService(config.Metrics.Enabled, config.Metrics.Path, config.Metrics.Port, logger)
					metrics.Start()

					logger.Info().Msg("Mount database")
					database, err := evmi_database.LoadDatabase(
						evmi_database.DatabaseType(config.Database.Type),
						config.Database.Config,
						logger,
					)

					if err != nil {
						return err
					}

					// if data.Backup.Enabled {
					// 	logger.Info().Msg("Mount backup service")
					// 	backups, err := backup.NewBackupService(database, data, logger)
					// 	if err != nil {
					// 		return err
					// 	}

					// 	logger.Info().Msg("Starting backup service")
					// 	backups.Start()
					// }

					// logger.Info().Msg("Mount hooks service")
					// hooks, err := hooks.NewHookService(internalBus, data, logger)
					// if err != nil {
					// 	return err
					// }

					// logger.Info().Msg("Starting hooks service")
					// hooks.Start()

					var instance evmi_database.EvmiInstance
					result := database.Conn.Model(&evmi_database.EvmiInstance{}).Where("instance_id = ?", instanceId).First(&instance)
					if result.Error != nil {
						log.Println(result.Error.Error())
						if result.Error.Error() == "record not found" {

							instance = evmi_database.EvmiInstance{
								InstanceId: instanceId,
							}

						} else {
							return result.Error
						}
					}

					instance.IpV4 = GetLocalIP().String()
					instance.Port = grpc.ServerPort
					instance.Status = "RUNNING"
					database.Conn.Save(&instance)

					// Provision config-declared resources before the services start,
					// so autoloaded sources/exporters are picked up on this boot.
					// Plugins are imported first so exporters can reference them by name.
					exporter.Configure(config.PluginStorage.BuildDir, config.PluginStorage.InstallDir)

					logger.Info().Msg("Import config plugins")
					exporter.ImportConfigPlugins(database, config.Plugins, logger)

					logger.Info().Msg("Autoload config resources")
					autoloader.Load(database, instance.ID, config.Resources, logger)

					logger.Info().Msg("Verify installed plugins")
					exporter.VerifyPlugins(database, logger)

					logger.Info().Msg("Mount indexer service")
					pipelineService := indexer.NewIndexerService(instanceId, database, internalBus, metrics, logger)

					logger.Info().Msg("Start pipeline service")
					err = pipelineService.Start()
					if err != nil {
						logger.Fatal().Msg(err.Error())
					}

					logger.Info().Msg("Mount exporter service")
					exporterService := exporter.NewExporterServiceManager(instanceId, database, internalBus, metrics, logger)

					logger.Info().Msg("Start exporter service")
					err = exporterService.Start()
					if err != nil {
						logger.Fatal().Msg(err.Error())
					}

					logger.Info().Msg("Start gRPC server")
					grpc.StartGrpcServer(config, database, internalBus, logger)
					return nil
				},
			},
			{
				Name:  "gateway",
				Usage: "Start the EVMI gateway: a stateful load balancer that forwards each request to the instance owning the data",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "config",
						Aliases: []string{"c"},
						EnvVars: []string{"CONFIG_FILE_PATH"},
						Value:   "/tmp/evm-indexer/config.json",
						Usage:   "Path to the config file (used only for the shared metadata database)",
					},
					&cli.Uint64Flag{
						Name:    "port",
						Aliases: []string{"p"},
						EnvVars: []string{"EVMI_GATEWAY_PORT"},
						Value:   8080,
						Usage:   "Port the gateway listens on",
					},
					&cli.Uint64Flag{
						Name:    "cache-ttl",
						EnvVars: []string{"EVMI_GATEWAY_CACHE_TTL"},
						Value:   30,
						Usage:   "Seconds to cache data-location lookups before re-reading the metadata DB",
					},
				},
				Action: func(cCtx *cli.Context) error {
					logger := zerolog.New(
						zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339},
					).Level(zerolog.TraceLevel).With().Timestamp().Caller().Logger()

					configFile, err := os.ReadFile(cCtx.String("config"))
					if err != nil {
						logger.Fatal().Msg(err.Error())
					}
					var config types.Config
					if err := json.Unmarshal(configFile, &config); err != nil {
						logger.Fatal().Msg(err.Error())
					}

					logger.Info().Msg("Mount database")
					database, err := evmi_database.LoadDatabase(
						evmi_database.DatabaseType(config.Database.Type),
						config.Database.Config,
						logger,
					)
					if err != nil {
						return err
					}

					gateway.StartGateway(
						database,
						cCtx.Uint64("port"),
						time.Duration(cCtx.Uint64("cache-ttl"))*time.Second,
						logger,
					)
					return nil
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		logger := zerolog.New(
			zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339},
		).Level(zerolog.TraceLevel).With().Timestamp().Caller().Logger()

		logger.Fatal().Msg(err.Error())
	}
}

func GetLocalIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddress := conn.LocalAddr().(*net.UDPAddr)

	return localAddress.IP
}
