package service

import (
	"sync"
)

var once sync.Once

type SysService struct {
}

func NewSysService() *SysService {
	sysSvc := &SysService{}
	once.Do(
		func() {

		})
	return sysSvc
}
