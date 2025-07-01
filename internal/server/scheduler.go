package server

import (
	"context"
	"fmt"
	"net"

	"dingoscheduler/internal/service"
	"dingoscheduler/pkg/config"
	"dingoscheduler/pkg/proto/manager"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type SchedulerServer struct {
	grpcServer     *grpc.Server
	managerService *service.ManagerService
}

func NewSchedulerServer(managerService *service.ManagerService) *SchedulerServer {
	return &SchedulerServer{
		managerService: managerService,
	}
}

func (s *SchedulerServer) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.SysConfig.Scheduler.Port))
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	manager.RegisterManagerServer(grpcServer, s.managerService)
	//
	// // 启动客户端状态检查协程
	// go server.checkClientActivity()
	if err := grpcServer.Serve(lis); err != nil {
		zap.S().Errorf("grpc server start fail: %v", err)
		return err
	}
	return nil
}

func (s *SchedulerServer) Stop(ctx context.Context) error {
	zap.S().Infof("[GRPC] server shutdown.")
	s.grpcServer.Stop()
	return nil
}
