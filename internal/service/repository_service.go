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
	"dingoscheduler/internal/model"
	"dingoscheduler/internal/model/dto"
	"dingoscheduler/internal/model/query"
	"dingoscheduler/pkg/common"
	myerr "dingoscheduler/pkg/error"
	"dingoscheduler/pkg/util"

	"github.com/bytedance/sonic"
	"github.com/labstack/echo/v4"
	"github.com/young2j/gocopy"
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
	zap.S().Debugf("PersistRepo instanceId:%s", query.InstanceIds)
	for _, instanceId := range query.InstanceIds {
		freeRepositories, err := s.repositoryDao.GetFreeRepository(instanceId)
		if err != nil {
			return err
		}
		if len(freeRepositories) == 0 {
			return nil
		}
		for _, repository := range freeRepositories {
			entity, err := s.dingospeedDao.GetEntity(instanceId, true)
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
			var metaData dto.CommitHfSha
			if err = sonic.Unmarshal(resp.Body, &metaData); err != nil {
				zap.S().Errorf("unmarshal error.%v", err)
				return err
			}
			repo := &model.Repository{
				InstanceId:    instanceId,
				Datatype:      repository.Datatype,
				Org:           repository.Org,
				Repo:          repository.Repo,
				OrgRepo:       orgRepo,
				LikeNum:       metaData.Likes,
				DownloadNum:   metaData.Downloads,
				PipelineTagId: metaData.PipelineTag,
				LastModified:  metaData.LastModified,
				UsedStorage:   metaData.UsedStorage,
			}
			tags := make([]*model.RepositoryTag, 0)
			for _, tag := range metaData.Tags {
				tags = append(tags, &model.RepositoryTag{
					TagId: tag,
				})
			}
			err = s.repositoryDao.RepoAndTagSave(repo, tags)
			if err != nil {
				zap.S().Errorf("repository save err.%v", err)
				return err
			}
		}
	}
	return nil
}

func (s *RepositoryService) ModelList(query *query.ModelQuery) ([]*dto.Repository, int64, error) {
	repositories, size, err := s.repositoryDao.ModelList(query)
	if err != nil {
		return nil, 0, err
	}
	repos := make([]*dto.Repository, 0)
	gocopy.Copy(&repos, &repositories)
	return repos, size, nil
}

func (s *RepositoryService) GetById(id int64) (*dto.Repository, error) {
	repository, err := s.repositoryDao.Get(id)
	if err != nil {
		return nil, err
	}
	var repos dto.Repository
	gocopy.Copy(&repos, &repository)

	// return repos, nil
	return nil, nil
}
