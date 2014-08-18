// Copyright 2014 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build appengine

package main

import (
	"github.com/barakmich/glog"

	"github.com/jsccast/cayley/config"
	"github.com/jsccast/cayley/db"
	"github.com/jsccast/cayley/graph"
	"github.com/jsccast/cayley/http"

	_ "github.com/jsccast/cayley/graph/memstore"
)

func init() {
	glog.SetToStderr(true)
	cfg := config.ParseConfigFromFile("cayley_appengine.cfg")
	ts, _ := graph.NewTripleStore("memstore", "", nil)
	glog.Errorln(cfg)
	db.Load(ts, cfg, cfg.DatabasePath)
	http.SetupRoutes(ts, cfg)
}
