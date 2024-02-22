package interfaces

type Orchestrator interface {
	Start() error
	Stop() error
	GetServer() Server

	TriggerSwitchover() error
}
