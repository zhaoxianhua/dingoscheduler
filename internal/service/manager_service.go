//  Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http:www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"dingoscheduler/internal/dao"
	"dingoscheduler/internal/model"
	myerr "dingoscheduler/pkg/error"
	pb "dingoscheduler/pkg/proto/manager"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ManagerService struct {
	pb.UnimplementedManagerServer
	clients            sync.Map // 使用 sync.Map 存储客户端信息
	dingospeedDao      *dao.DingospeedDao
	modelFileRecordDao *dao.ModelFileRecordDao
}

func NewManagerService(dingospeedDao *dao.DingospeedDao, modelFileRecordDao *dao.ModelFileRecordDao) *ManagerService {
	return &ManagerService{
		dingospeedDao:      dingospeedDao,
		modelFileRecordDao: modelFileRecordDao,
	}
}

func (s *ManagerService) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	dingospeed := &model.Dingospeed{
		Area:      req.Area,
		Host:      req.Host,
		Port:      req.Port,
		UpdatedAt: time.Now(),
		CreatedAt: time.Now(),
	}
	speed, err := s.dingospeedDao.GetEntity(req.Area, req.Host, req.Port)
	if err != nil {
		zap.S().Errorf("getEntity err.%v", err)
		return nil, err
	}
	if speed != nil {
		dingospeed.ID = speed.ID
		err = s.dingospeedDao.Update(*dingospeed)
		if err != nil {
			return nil, err
		}
	} else {
		err = s.dingospeedDao.Save(*dingospeed)
		if err != nil {
			return nil, err
		}
	}
	return &pb.RegisterResponse{
		Success: true,
		Id:      dingospeed.ID,
	}, nil
}

func (s *ManagerService) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*emptypb.Empty, error) {
	if req.Id > 0 {
		err := s.dingospeedDao.UpdateForMap(req.Id, map[string]interface{}{"updated_at": time.Now()})
		if err != nil {
			return nil, err
		}
	} else {
		return nil, myerr.New(fmt.Sprintf("speed id is unlawful.id = %d", req.Id))
	}
	return nil, nil
}

func (s *ManagerService) ReportCompleteFile(ctx context.Context, req *pb.CompleteFileRequest) (*emptypb.Empty, error) {
	if len(req.CompleteFiles) == 0 {
		return nil, nil
	}
	records := make([]model.ModelFileRecord, 0)
	for _, item := range req.CompleteFiles {
		m := model.ModelFileRecord{
			Datatype:     item.DataType,
			Org:          item.Org,
			Repo:         item.Repo,
			Path:         item.Path,
			DingospeedID: item.DingospeedId,
			CompleteAt:   time.Now(),
		}
		records = append(records, m)
	}
	err := s.modelFileRecordDao.BatchSave(records)
	if err != nil {
		return nil, err
	}
	return nil, nil
}
