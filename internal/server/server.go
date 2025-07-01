package server

import "github.com/google/wire"

var ServerProvider = wire.NewSet(NewHTTPServer, NewSchedulerServer, NewEngine)
