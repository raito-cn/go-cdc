package cannal

type IncrementalService interface {
	Run()
	Stop()
	IsRunning() bool
}
