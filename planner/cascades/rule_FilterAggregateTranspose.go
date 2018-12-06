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
	"container/list"
	"fmt"

	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
)

/**
type Transformation interface {
	GetPattern() *Pattern
	Match(expr *ExprIter) (matched bool)
	OnTransform(old *ExprIter) (new *GroupExpr, eraseOld bool, err error)
}
*/

type baseRuleImpl struct {
	pattern *Pattern
}

func (r *baseRuleImpl) GetPattern() *Pattern {
	return r.pattern
}

func (r *baseRuleImpl) Match(expr *ExprIter) bool {
	return true
}

type FilterAggregateTransposeRule struct {
	baseRuleImpl
}

func NewFilterAggregateTransposeRule() *FilterAggregateTransposeRule {
	pattern := BuildPattern(OperandSelection, BuildPattern(OperandAggregation))
	return &FilterAggregateTransposeRule{baseRuleImpl{pattern}}
}

func (r *FilterAggregateTransposeRule) OnTransform(pctx *plannerContext, old *ExprIter) (new *GroupExpr, eraseOld bool, err error) {
	sel := old.GetLogicalPlan().(*plannercore.LogicalSelection)
	agg := old.children[0].GetLogicalPlan().(*plannercore.LogicalAggregation)

	gbyCols := make(map[int64]struct{}, len(agg.GroupByItems))
	for i := range agg.GroupByItems {
		gbyCol, isCol := agg.GroupByItems[i].(*expression.Column)
		if !isCol {
			return nil, false, nil
		}
		gbyCols[gbyCol.UniqueID] = struct{}{}
	}

	cols = make([]int64, 0)
	availConds := make([]expression.Expression, 0, len(sel.Conditions))
	for i := len(sel.Conditions) - 1; i >= 0; i-- {
		cols = expression.GetCols(sel.Conditions[i], cols[:0])
		allFound := true
		for _, uniqueID := range cols {
			_, ok := gbyCols[uniqueID]
			if !ok {
				allFound = false
				break
			}
		}
		if allFound {
			availConds = append(availConds, sel.Conditions[i])
			sel.Conditions = append(sel.Conditions[:i], sel.Conditions[i+1:])
		}
	}

	if len(availConds) == 0 {
		return nil, false, nil
	}

}
