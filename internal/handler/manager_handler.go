package handler

import (
	"dingoscheduler/internal/service"
)

type ManagerHandler struct {
	managerService *service.ManagerService
}

func NewManagerHandler(managerService *service.ManagerService) *ManagerHandler {
	return &ManagerHandler{
		managerService: managerService,
	}
}
