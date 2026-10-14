//go:build !unix

package observability

// ProcessCPUSeconds is not implemented on this platform.
func ProcessCPUSeconds() float64 { return 0 }
