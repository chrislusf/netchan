package util

import ()

type ControlFlag byte

const (
	Data ControlFlag = iota
	CloseChannel
	FullStop
)
