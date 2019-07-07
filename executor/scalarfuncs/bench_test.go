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
	"testing"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/vector"
)

func BenchmarkScalarFuncPlus(b *testing.B) {
	col0 := vector.NewVecInt64(1024)
	for i := 0; i < 1024; i++ {
		col0.Values[i] = int64(i)
	}
	col1 := vector.NewVecInt64(1024)
	for i := 0; i < 1024; i++ {
		col1.Values[i] = int64(i)
	}

	colType := &types.FieldType{
		Tp:   mysql.TypeLonglong,
		Flen: mysql.MaxIntWidth,
	}

	colRef0 := ColRefInt64{
		baseScalarFunc: newBaseScalarFunc("ColRef_0", colType),
		colIdx:         0,
		vec:            vector.Vector(col0),
	}
	colRef1 := ColRefInt64{
		baseScalarFunc: newBaseScalarFunc("ColRef_1", colType),
		colIdx:         1,
		vec:            vector.Vector(col1),
	}
	funcPlus := PlusInt64{
		baseScalarFunc: newBaseScalarFunc("plus", colType, &colRef0, &colRef1),
		lhs:            vector.NewVecInt64(1024),
		rhs:            vector.NewVecInt64(1024),
	}

	res := vector.NewVecInt64(1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := funcPlus.Execute(vector.Vector(res))
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkSimplePlus(bench *testing.B) {
	var numRows = 1024
	a := make([]int64, numRows)
	b := make([]int64, numRows)
	c := make([]int64, numRows)

	bench.ResetTimer()
	for i := 0; i < bench.N; i++ {
		for i := 0; i < numRows; i++ {
			c[i] = a[i] + b[i]
		}
	}
}
