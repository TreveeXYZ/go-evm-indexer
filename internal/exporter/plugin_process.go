package exporter

import (
	"errors"
	"fmt"
	"os"
	"os/exec"

	"github.com/evmi-cloud/go-evm-indexer/internal/logsafe"
	pluginsdk "github.com/evmi-cloud/go-evm-indexer/pkg/exporter"
	"github.com/hashicorp/go-hclog"
	goplugin "github.com/hashicorp/go-plugin"
	"github.com/rs/zerolog"
)

// pluginProcess is a launched plugin subprocess plus the gRPC handle EVMI calls
// it through. The process lives exactly as long as this value: whoever starts one
// MUST call Kill (deferred) or the child is leaked for the life of the server.
type pluginProcess struct {
	client *goplugin.Client
	plugin *pluginsdk.GRPCClient
}

// Exporter is the plugin as the export loop sees it.
func (p *pluginProcess) Exporter() pluginsdk.Exporter { return p.plugin }

// SetHost installs the EVMI functions this plugin may call back into. It must be
// called before Init, which is where the host service is stood up and its broker
// id handed to the plugin.
func (p *pluginProcess) SetHost(h pluginsdk.Host) { p.plugin.SetHost(h) }

// ConfigSchema asks the plugin for its declared config parameters. The bool
// reports whether the plugin implements exporter.Configurable at all.
func (p *pluginProcess) ConfigSchema() ([]pluginsdk.ConfigField, bool, error) {
	return p.plugin.ConfigSchema()
}

// Exited reports whether the plugin process is gone. The export loop polls it so
// a plugin that dies while idle (between blocks, with no call in flight to fail)
// is noticed instead of leaving the exporter marked RUNNING against a dead
// process.
func (p *pluginProcess) Exited() bool {
	return p != nil && p.client != nil && p.client.Exited()
}

// Kill terminates the plugin process. Safe to call more than once.
func (p *pluginProcess) Kill() {
	if p != nil && p.client != nil {
		p.client.Kill()
	}
}

// startPlugin launches an installed plugin binary as a subprocess and dispenses
// its exporter over gRPC (hashicorp/go-plugin).
//
// The binary is an ordinary executable built with any Go toolchain, it runs in
// its own address space (a panic kills the plugin, not the indexer), and it can
// be terminated when its exporter stops.
func startPlugin(binaryPath string, name string, logger zerolog.Logger) (*pluginProcess, error) {
	info, err := os.Stat(binaryPath)
	if err != nil {
		return nil, fmt.Errorf("plugin binary %s: %w", binaryPath, err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("plugin binary %s is a directory", binaryPath)
	}

	client := goplugin.NewClient(&goplugin.ClientConfig{
		HandshakeConfig: pluginsdk.Handshake,
		Plugins:         pluginsdk.PluginMap(nil),
		Cmd:             exec.Command(binaryPath),
		// The plugin type only implements gRPC; refusing net/rpc up front turns a
		// protocol mismatch into a clear handshake error.
		AllowedProtocols: []goplugin.Protocol{goplugin.ProtocolGRPC},
		// Mutual TLS with a per-launch certificate, so nothing else on the host can
		// talk to the plugin's socket.
		AutoMTLS: true,
		Logger:   pluginLogger(name),
	})

	rpcClient, err := client.Client()
	if err != nil {
		client.Kill()
		return nil, fmt.Errorf("starting plugin %s: %w", binaryPath, err)
	}

	raw, err := rpcClient.Dispense(pluginsdk.PluginName)
	if err != nil {
		client.Kill()
		return nil, fmt.Errorf("dispensing plugin %s: %w", binaryPath, err)
	}

	instance, ok := raw.(*pluginsdk.GRPCClient)
	if !ok {
		client.Kill()
		return nil, fmt.Errorf("plugin %s served an unexpected type %T", binaryPath, raw)
	}

	logger.Info().Str("plugin", name).Str("binary", binaryPath).Msg("plugin process started")
	return &pluginProcess{client: client, plugin: instance}, nil
}

// pluginLogger is the hclog logger go-plugin needs. It writes to stderr, where
// the server's zerolog output also goes, and carries the plugin name so both
// go-plugin's own lines and anything the plugin writes to its stderr are
// attributable.
func pluginLogger(name string) hclog.Logger {
	if name == "" {
		name = "plugin"
	}
	return hclog.New(&hclog.LoggerOptions{
		Name:   "plugin/" + name,
		Output: logsafe.Stderr,
		Level:  hclog.Info,
	})
}

// errPluginNotInstalled is returned when an exporter references a plugin that has
// no usable binary.
var errPluginNotInstalled = errors.New("plugin is not installed")
