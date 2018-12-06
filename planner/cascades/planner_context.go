// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cascades

import (
	"github.com/pingcap/tidb/sessionctx"
)

type plannerContext struct {
	sctx sessionctx.Context

	// groupExpr2Group mapes the fingerprint of a GroupExpr to Group.
	groupExpr2Group map[string]*Group
}

func newPlannerContext(sctx sessionctx.Context) *plannerContext {
	return &plannerContext{
		sctx:            sctx,
		groupExpr2Group: make(map[string]*Group),
	}
}

func (c *plannerContext) getGroupByGroupExpr(fingerprint string) *Group {
	return c.groupExpr2Group[fingerprint]
}

func (c *plannerContext) putGroupExprToGroup(fingerprint string, g *Group) {
	c.groupExpr2Group[fingerprint] = g
}
