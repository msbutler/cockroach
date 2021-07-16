// Code generated by execgen; DO NOT EDIT.
// Copyright 2019 The Cockroach Authors.
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"bytes"
	"math"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ = typeconv.DatumVecCanonicalTypeFamily
	_ coldataext.Datum
	_ tree.AggType
)

// valuesDiffer takes in two ColVecs as well as values indices to check whether
// the values differ. This function pays attention to NULLs, and two NULL
// values do *not* differ.
func valuesDiffer(aColVec coldata.Vec, aValueIdx int, bColVec coldata.Vec, bValueIdx int) bool {
	switch aColVec.CanonicalTypeFamily() {
	case types.BoolFamily:
		switch aColVec.Type().Width() {
		case -1:
		default:
			aCol := aColVec.Bool()
			bCol := bColVec.Bool()
			aNulls := aColVec.Nulls()
			bNulls := bColVec.Nulls()
			aNull := aNulls.MaybeHasNulls() && aNulls.NullAt(aValueIdx)
			bNull := bNulls.MaybeHasNulls() && bNulls.NullAt(bValueIdx)
			if aNull && bNull {
				return false
			} else if aNull || bNull {
				return true
			}
			arg1 := aCol.Get(aValueIdx)
			arg2 := bCol.Get(bValueIdx)
			var unique bool

			{
				var cmpResult int

				if !arg1 && arg2 {
					cmpResult = -1
				} else if arg1 && !arg2 {
					cmpResult = 1
				} else {
					cmpResult = 0
				}

				unique = cmpResult != 0
			}

			return unique
		}
	case types.BytesFamily:
		switch aColVec.Type().Width() {
		case -1:
		default:
			aCol := aColVec.Bytes()
			bCol := bColVec.Bytes()
			aNulls := aColVec.Nulls()
			bNulls := bColVec.Nulls()
			aNull := aNulls.MaybeHasNulls() && aNulls.NullAt(aValueIdx)
			bNull := bNulls.MaybeHasNulls() && bNulls.NullAt(bValueIdx)
			if aNull && bNull {
				return false
			} else if aNull || bNull {
				return true
			}
			arg1 := aCol.Get(aValueIdx)
			arg2 := bCol.Get(bValueIdx)
			var unique bool

			{
				var cmpResult int
				cmpResult = bytes.Compare(arg1, arg2)
				unique = cmpResult != 0
			}

			return unique
		}
	case types.DecimalFamily:
		switch aColVec.Type().Width() {
		case -1:
		default:
			aCol := aColVec.Decimal()
			bCol := bColVec.Decimal()
			aNulls := aColVec.Nulls()
			bNulls := bColVec.Nulls()
			aNull := aNulls.MaybeHasNulls() && aNulls.NullAt(aValueIdx)
			bNull := bNulls.MaybeHasNulls() && bNulls.NullAt(bValueIdx)
			if aNull && bNull {
				return false
			} else if aNull || bNull {
				return true
			}
			arg1 := aCol.Get(aValueIdx)
			arg2 := bCol.Get(bValueIdx)
			var unique bool

			{
				var cmpResult int
				cmpResult = tree.CompareDecimals(&arg1, &arg2)
				unique = cmpResult != 0
			}

			return unique
		}
	case types.IntFamily:
		switch aColVec.Type().Width() {
		case 16:
			aCol := aColVec.Int16()
			bCol := bColVec.Int16()
			aNulls := aColVec.Nulls()
			bNulls := bColVec.Nulls()
			aNull := aNulls.MaybeHasNulls() && aNulls.NullAt(aValueIdx)
			bNull := bNulls.MaybeHasNulls() && bNulls.NullAt(bValueIdx)
			if aNull && bNull {
				return false
			} else if aNull || bNull {
				return true
			}
			arg1 := aCol.Get(aValueIdx)
			arg2 := bCol.Get(bValueIdx)
			var unique bool

			{
				var cmpResult int

				{
					a, b := int64(arg1), int64(arg2)
					if a < b {
						cmpResult = -1
					} else if a > b {
						cmpResult = 1
					} else {
						cmpResult = 0
					}
				}

				unique = cmpResult != 0
			}

			return unique
		case 32:
			aCol := aColVec.Int32()
			bCol := bColVec.Int32()
			aNulls := aColVec.Nulls()
			bNulls := bColVec.Nulls()
			aNull := aNulls.MaybeHasNulls() && aNulls.NullAt(aValueIdx)
			bNull := bNulls.MaybeHasNulls() && bNulls.NullAt(bValueIdx)
			if aNull && bNull {
				return false
			} else if aNull || bNull {
				return true
			}
			arg1 := aCol.Get(aValueIdx)
			arg2 := bCol.Get(bValueIdx)
			var unique bool

			{
				var cmpResult int

				{
					a, b := int64(arg1), int64(arg2)
					if a < b {
						cmpResult = -1
					} else if a > b {
						cmpResult = 1
					} else {
						cmpResult = 0
					}
				}

				unique = cmpResult != 0
			}

			return unique
		case -1:
		default:
			aCol := aColVec.Int64()
			bCol := bColVec.Int64()
			aNulls := aColVec.Nulls()
			bNulls := bColVec.Nulls()
			aNull := aNulls.MaybeHasNulls() && aNulls.NullAt(aValueIdx)
			bNull := bNulls.MaybeHasNulls() && bNulls.NullAt(bValueIdx)
			if aNull && bNull {
				return false
			} else if aNull || bNull {
				return true
			}
			arg1 := aCol.Get(aValueIdx)
			arg2 := bCol.Get(bValueIdx)
			var unique bool

			{
				var cmpResult int

				{
					a, b := int64(arg1), int64(arg2)
					if a < b {
						cmpResult = -1
					} else if a > b {
						cmpResult = 1
					} else {
						cmpResult = 0
					}
				}

				unique = cmpResult != 0
			}

			return unique
		}
	case types.FloatFamily:
		switch aColVec.Type().Width() {
		case -1:
		default:
			aCol := aColVec.Float64()
			bCol := bColVec.Float64()
			aNulls := aColVec.Nulls()
			bNulls := bColVec.Nulls()
			aNull := aNulls.MaybeHasNulls() && aNulls.NullAt(aValueIdx)
			bNull := bNulls.MaybeHasNulls() && bNulls.NullAt(bValueIdx)
			if aNull && bNull {
				return false
			} else if aNull || bNull {
				return true
			}
			arg1 := aCol.Get(aValueIdx)
			arg2 := bCol.Get(bValueIdx)
			var unique bool

			{
				var cmpResult int

				{
					a, b := float64(arg1), float64(arg2)
					if a < b {
						cmpResult = -1
					} else if a > b {
						cmpResult = 1
					} else if a == b {
						cmpResult = 0
					} else if math.IsNaN(a) {
						if math.IsNaN(b) {
							cmpResult = 0
						} else {
							cmpResult = -1
						}
					} else {
						cmpResult = 1
					}
				}

				unique = cmpResult != 0
			}

			return unique
		}
	case types.TimestampTZFamily:
		switch aColVec.Type().Width() {
		case -1:
		default:
			aCol := aColVec.Timestamp()
			bCol := bColVec.Timestamp()
			aNulls := aColVec.Nulls()
			bNulls := bColVec.Nulls()
			aNull := aNulls.MaybeHasNulls() && aNulls.NullAt(aValueIdx)
			bNull := bNulls.MaybeHasNulls() && bNulls.NullAt(bValueIdx)
			if aNull && bNull {
				return false
			} else if aNull || bNull {
				return true
			}
			arg1 := aCol.Get(aValueIdx)
			arg2 := bCol.Get(bValueIdx)
			var unique bool

			{
				var cmpResult int

				if arg1.Before(arg2) {
					cmpResult = -1
				} else if arg2.Before(arg1) {
					cmpResult = 1
				} else {
					cmpResult = 0
				}
				unique = cmpResult != 0
			}

			return unique
		}
	case types.IntervalFamily:
		switch aColVec.Type().Width() {
		case -1:
		default:
			aCol := aColVec.Interval()
			bCol := bColVec.Interval()
			aNulls := aColVec.Nulls()
			bNulls := bColVec.Nulls()
			aNull := aNulls.MaybeHasNulls() && aNulls.NullAt(aValueIdx)
			bNull := bNulls.MaybeHasNulls() && bNulls.NullAt(bValueIdx)
			if aNull && bNull {
				return false
			} else if aNull || bNull {
				return true
			}
			arg1 := aCol.Get(aValueIdx)
			arg2 := bCol.Get(bValueIdx)
			var unique bool

			{
				var cmpResult int
				cmpResult = arg1.Compare(arg2)
				unique = cmpResult != 0
			}

			return unique
		}
	case typeconv.DatumVecCanonicalTypeFamily:
		switch aColVec.Type().Width() {
		case -1:
		default:
			aCol := aColVec.Datum()
			bCol := bColVec.Datum()
			aNulls := aColVec.Nulls()
			bNulls := bColVec.Nulls()
			aNull := aNulls.MaybeHasNulls() && aNulls.NullAt(aValueIdx)
			bNull := bNulls.MaybeHasNulls() && bNulls.NullAt(bValueIdx)
			if aNull && bNull {
				return false
			} else if aNull || bNull {
				return true
			}
			arg1 := aCol.Get(aValueIdx)
			arg2 := bCol.Get(bValueIdx)
			var unique bool

			{
				var cmpResult int

				cmpResult = arg1.(*coldataext.Datum).CompareDatum(aCol, arg2)

				unique = cmpResult != 0
			}

			return unique
		}
	}
	colexecerror.InternalError(errors.AssertionFailedf("unsupported valuesDiffer type %s", aColVec.Type()))
	// This code is unreachable, but the compiler cannot infer that.
	return false
}
