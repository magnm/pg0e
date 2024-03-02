package cnpg

import "github.com/magnm/pg0e/pkg/interfaces"

type CNPGOrchestrator struct {
	server interfaces.Server
}

func New(server interfaces.Server) *CNPGOrchestrator {
	return &CNPGOrchestrator{
		server: server,
	}
}

func (c *CNPGOrchestrator) Start() error {
	return nil
}

func (c *CNPGOrchestrator) Stop() error {
	return nil
}

func (c *CNPGOrchestrator) GetServer() interfaces.Server {
	return c.server
}

func (c *CNPGOrchestrator) TriggerSwitchover() error {
	return nil
}
