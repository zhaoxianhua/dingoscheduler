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
	"fmt"
	"net/http"

	"dingoscheduler/internal/dao"
	"dingoscheduler/internal/model/dto"
	"dingoscheduler/internal/model/query"
	"dingoscheduler/pkg/common"
	myerr "dingoscheduler/pkg/error"
	"dingoscheduler/pkg/util"

	"github.com/bytedance/sonic"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

type RepositoryService struct {
	dingospeedDao       *dao.DingospeedDao
	modelFileProcessDao *dao.ModelFileProcessDao
	repositoryDao       *dao.RepositoryDao
	Pool                *common.Pool
}

func NewRepositoryService(dingospeedDao *dao.DingospeedDao, modelFileProcessDao *dao.ModelFileProcessDao,
	repositoryDao *dao.RepositoryDao) *RepositoryService {
	return &RepositoryService{
		dingospeedDao:       dingospeedDao,
		repositoryDao:       repositoryDao,
		modelFileProcessDao: modelFileProcessDao,
		Pool:                common.NewPool(30),
	}
}

func (s *RepositoryService) PersistRepo(c echo.Context, query *query.PersistRepoQuery) error {
	zap.S().Debugf("Preheat:%s, %s/%s/%s/%s", query.Area)
	freeRepositories, err := s.repositoryDao.GetFreeRepository()
	if err != nil {
		return err
	}
	if len(freeRepositories) == 0 {
		return nil
	}
	for _, area := range query.Area {
		for _, repository := range freeRepositories {
			entity, err := s.dingospeedDao.GetEntity(area, true)
			if err != nil {
				return err
			}
			if entity == nil {
				return myerr.New("该区域dingspeed未注册。")
			}
			speedDomain := fmt.Sprintf("http://%s:%d", entity.Host, entity.Port)
			orgRepo := util.GetOrgRepo(repository.Org, repository.Repo)
			resp, err := s.dingospeedDao.RemoteRequestMeta(speedDomain, repository.Datatype, orgRepo, "main", query.Token)
			if err != nil {
				return err
			}
			if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusTemporaryRedirect {
				return myerr.NewAppendCode(resp.StatusCode, "RemoteRequestMeta err")
			}
			var sha dto.CommitHfSha
			if err = sonic.Unmarshal(resp.Body, &sha); err != nil {
				zap.S().Errorf("unmarshal error.%v", err)
				return err
			}
			// repo := model.Repository{
			// 	Org:       repository.Org,
			// 	Repo:      repository.Repo,
			// 	OrgRepo:   orgRepo,
			// 	LikeNum:   sha.Likes,
			// 	FollowNum: sha.Downloads,
			// }
		}
	}

	// jobModel := model.PreheatJob{
	// 	Area:      job.Area,
	// 	Datatype:  job.Datatype,
	// 	Org:       job.Org,
	// 	Repo:      job.Repo,
	// 	Token:     job.Token,
	// 	Revision:  sha.Sha,
	// 	FileTotal: len(sha.Siblings),
	// 	MetaInfo:  string(resp.Body),
	// 	Status:    consts.StatusPreheatCommit,
	// }
	// if err = s.Pool.Submit(context.Background(), &PreheatTask{
	// 	preheatJobDao:       s.preheatJobDao,
	// 	modelFileProcessDao: s.modelFileProcessDao,
	// 	Job:                 &jobModel,
	// 	Domain:              speedDomain,
	// }); err != nil {
	// 	return err
	// }
	// err = s.preheatJobDao.Save(&jobModel)
	// if err != nil {
	// 	return err
	// }
	return nil
}
