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
	"github.com/pingcap/tidb/util/set"
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

// OnTransform push the filters through the aggregate:
// before: filter(aggregate(any), f1, f2, ...)
// after:  aggregate(filter(any, f1, f2, ...)),      totaly push through
//    or:  filter(aggregate(filter(any, f1)), f2), partialy push through
func (r *FilterAggregateTransposeRule) OnTransform(pctx *plannerContext, old *ExprIter) (new *GroupExpr, eraseOld bool, err error) {
	sel := old.GetLogicalPlan().(*plannercore.LogicalSelection)
	agg := old.children[0].GetLogicalPlan().(*plannercore.LogicalAggregation)

	gbyColIds = r.collectGbyCols(agg)

	var pushed []expression.Expression
	for i := len(sel.Conditions) - 1; i >= 0; i-- {
		if !r.coveredByGbyCols(sel.Conditions[i], gbyColIds) {
			continue
		}
		if pushed == nil {
			pushed = make([]expression.Expression, 0, i)
		}
		pushed = append(pushed, sel.Conditions[i])
		sel.Conditions = append(sel.Conditions[0:i], sel.Conditions[i+1:]...)
	}

	if len(pushed) == 0 {
		return nil, false, nil
	}

	newSelGroupExpr := groupExprBuilder.buildSelection(pctx, pushed)
	newSelGroupExpr.children = old.children[0].GetGroupExpr().children
	if pctx.Exists(newSelGroupExpr.FingerPrint()) {
		return nil, false, nil
	}

	// deduplicate group expr
	//
	// construct new group
	// construct group logical property
	newSel := newSelGroupExpr.GetLogicalPlan().(*plannercore.LogicalSelection)
	newSel.SetSchema(agg.children[0].Schema())
}

func (r *FilterAggregateTransposeRule) collectGbyCols(agg *plannercore.LogicalAggregation) set.Int64Set {
	gbyColIds := set.NewInt64Set()
	for i := range agg.GroupByItems {
		gbyCol, isCol := agg.GroupByItems[i].(*expression.Column)
		if isCol {
			gbyColIds.Insert(gbyCol.UniqueID)
		}
	}
	return gbyColIds
}

func (r *FilterAggregateTransposeRule) coveredByGbyCols(filter expression.Expression, gbyColIds set.Int64Set) bool {
	switch v := filter.(type) {
	case *Column:
		return gbyColIds.Exist(v.UniqueID)
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			if !r.coveredByGbyCols(arg, gbyColIds) {
				return false
			}
		}
	}
	return true
}
