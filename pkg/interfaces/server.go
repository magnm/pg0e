package interfaces

type Server interface {
	SetOrchestrator(orchestrator Orchestrator)

	InitiateSwitch()
	UnpauseAll()
}
