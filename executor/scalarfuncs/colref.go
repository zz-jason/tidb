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
	"github.com/pingcap/tidb/util/vector"
)

// ColRefInt64 represents the ScalarFunc for accessing a column.
type ColRefInt64 struct {
	*baseScalarFunc
	vec    vector.Vector // Vector should be reset before each runing round.
	colIdx int
}

// ColRefUInt64 represents the ScalarFunc for accessing a column.
type ColRefUInt64 struct {
	*baseScalarFunc
	vec    vector.Vector // Vector should be reset before each runing round.
	colIdx int
}

// ColRefFloat64 represents the ScalarFunc for accessing a column.
type ColRefFloat64 struct {
	*baseScalarFunc
	vec    vector.Vector // Vector should be reset before each runing round.
	colIdx int
}

// ColRefUFloat64 represents the ScalarFunc for accessing a column.
type ColRefUFloat64 struct {
	*baseScalarFunc
	vec    vector.Vector // Vector should be reset before each runing round.
	colIdx int
}

// ColRefDecimal represents the ScalarFunc for accessing a column.
type ColRefDecimal struct {
	*baseScalarFunc
	vec    vector.Vector // Vector should be reset before each runing round.
	colIdx int
}

// Execute implements the ScalarFunc.Execute interface.
func (f *ColRefInt64) Execute(vec vector.Vector) (err error) {
	in := (*vector.VecInt64)(f.vec)
	res := (*vector.VecInt64)(vec)

	res.NullBitmap.Copy(in.NullBitmap)
	for i := 0; i < len(res.Values); i++ {
		res.Values[i] = in.Values[i]
	}
	return nil
}
