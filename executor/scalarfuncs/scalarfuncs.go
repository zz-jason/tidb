// Copyright 2019 PingCAP, Inc.
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

package scalarfuncs

import (
	"fmt"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/vector"
)

// ScalarFunc represents a scalar function which is executed vectorized.
type ScalarFunc interface {
	Name() string
	RetType() *types.FieldType
	Execute(vec vector.Vector) error
}

type baseScalarFunc struct {
	name     string
	retType  *types.FieldType
	children []ScalarFunc
}

func newBaseScalarFunc(name string, retType *types.FieldType, children ...ScalarFunc) *baseScalarFunc {
	return &baseScalarFunc{
		name:     name,
		retType:  retType,
		children: children,
	}
}

// Name implements the ScalarFunc.Name interface.
func (f *baseScalarFunc) Name() string {
	return f.name
}

// RetType implements the ScalarFunc.RetType interface.
func (f *baseScalarFunc) RetType() *types.FieldType {
	return f.retType
}

// Execute implements the ScalarFunc.Execute interface.
func (f *baseScalarFunc) Execute(vec vector.Vector) error {
	return errors.Errorf("Function %v is not supported", f.name)
}

// PlusInt64 represents the ScalarFunc for ast.Plus.
type PlusInt64 struct {
	*baseScalarFunc
	lhs *vector.VecInt64
	rhs *vector.VecInt64
}

// PlusUInt64 represents the ScalarFunc for ast.Plus.
type PlusUInt64 struct{ *baseScalarFunc }

// PlusFloat64 represents the ScalarFunc for ast.Plus.
type PlusFloat64 struct{ *baseScalarFunc }

// PlusUFloat64 represents the ScalarFunc for ast.Plus.
type PlusUFloat64 struct{ *baseScalarFunc }

// PlusDecimal represents the ScalarFunc for ast.Plus.
type PlusDecimal struct{ *baseScalarFunc }

// Execute implements the ScalarFunc.Execute interface.
func (f *PlusInt64) Execute(vec vector.Vector) (err error) {
	err = f.children[0].Execute(vector.Vector(f.lhs))
	if err != nil {
		return err
	}

	err = f.children[1].Execute(vector.Vector(f.rhs))
	if err != nil {
		return err
	}

	res := (*vector.VecInt64)(vec)
	res.NullBitmap.Copy(f.lhs.NullBitmap)
	res.NullBitmap.Union(f.rhs.NullBitmap)
	for i := 0; i < len(res.Values); i++ {
		if (f.lhs.Values[i] > 0 && f.rhs.Values[i] > math.MaxInt64-f.lhs.Values[i]) ||
			(f.lhs.Values[i] < 0 && f.rhs.Values[i] < math.MinInt64-f.lhs.Values[i]) {
			return types.ErrOverflow.GenWithStackByArgs(
				"BIGINT",
				fmt.Sprintf("(%s + %s)", f.children[0].Name(), f.children[1].Name()),
			)
		}
		res.Values[i] = f.lhs.Values[i] + f.rhs.Values[i]
	}
	return nil
}
