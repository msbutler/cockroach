// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package quantize

import (
	"slices"

	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// RaBitQCode is a quantization code that partially encodes a quantized vector.
// It has 1 bit per dimension of the quantized vector it represents. For
// example, if the quantized vector has 512 dimensions, then its code will have
// 512 bits that are packed into uint64 values using big-endian ordering (i.e.
// a width of 64 bytes). If the dimensions are not evenly divisible by 64, the
// trailing bits of the code are set to zero.
type RaBitQCode []uint64

// MakeRaBitQCodeSet returns an empty set of quantization codes, where each code
// in the set represents a quantized vector with the given number of dimensions.
func MakeRaBitQCodeSet(dims int) RaBitQCodeSet {
	return RaBitQCodeSet{
		Count: 0,
		// Calculate the number of uint64 values needed to store 1 bit per
		// dimension.
		Width: (dims + 63) / 64,
	}
}

// MakeRaBitQCodeSetFromRawData constructs a set of quantization codes from a
// raw slice of codes. The raw codes are packed contiguously in memory and
// represent quantized vectors having the given number of dimensions.
// NB: The data slice is directly used rather than copied; do not use it outside
// the context of this code set after this point.
func MakeRaBitQCodeSetFromRawData(data []uint64, width int) RaBitQCodeSet {
	if len(data)%width != 0 {
		panic(errors.AssertionFailedf(
			"data length %d is not a multiple of the width %d", len(data), width))
	}
	return RaBitQCodeSet{Count: len(data) / width, Width: width, Data: data}
}

// At returns the code at the given position in the set as a slice of uint64
// values that can be read or written by the caller.
func (cs *RaBitQCodeSet) At(offset int) RaBitQCode {
	start := offset * cs.Width
	return cs.Data[start : start+cs.Width]
}

// Add appends the given code to this set.
func (cs *RaBitQCodeSet) Add(code RaBitQCode) {
	if len(code) != cs.Width {
		panic(errors.AssertionFailedf(
			"cannot add code with %d width to set with width %d", len(code), cs.Width))
	}
	cs.Data = append(cs.Data, code...)
	cs.Count++
}

// AddUndefined adds the given number of codes to this set. The codes should be
// set to defined values before use.
func (cs *RaBitQCodeSet) AddUndefined(count int) {
	cs.Data = slices.Grow(cs.Data, count*cs.Width)
	cs.Count += count
	cs.Data = cs.Data[:cs.Count*cs.Width]
}

// ReplaceWithLast removes the code at the given offset from the set, replacing
// it with the last code in the set. The modified set has one less element and
// the last code's position changes.
func (cs *RaBitQCodeSet) ReplaceWithLast(offset int) {
	targetStart := offset * cs.Width
	sourceEnd := len(cs.Data)
	copy(cs.Data[targetStart:targetStart+cs.Width], cs.Data[sourceEnd-cs.Width:sourceEnd])
	cs.Data = cs.Data[:sourceEnd-cs.Width]
	cs.Count--
}

// GetCount implements the QuantizedVectorSet interface.
func (vs *RaBitQuantizedVectorSet) GetCount() int {
	return len(vs.CodeCounts)
}

// GetCentroid implements the QuantizedVectorSet interface.
func (vs *RaBitQuantizedVectorSet) GetCentroid() vector.T {
	return vs.Centroid
}

// GetCentroidDistances implements the QuantizedVectorSet interface.
func (vs *RaBitQuantizedVectorSet) GetCentroidDistances() []float32 {
	return vs.CentroidDistances
}

// ReplaceWithLast implements the QuantizedVectorSet interface.
func (vs *RaBitQuantizedVectorSet) ReplaceWithLast(offset int) {
	lastOffset := len(vs.CodeCounts) - 1
	vs.Codes.ReplaceWithLast(offset)
	vs.CodeCounts[offset] = vs.CodeCounts[lastOffset]
	vs.CodeCounts = vs.CodeCounts[:lastOffset]
	vs.CentroidDistances[offset] = vs.CentroidDistances[lastOffset]
	vs.CentroidDistances = vs.CentroidDistances[:lastOffset]
	vs.DotProducts[offset] = vs.DotProducts[lastOffset]
	vs.DotProducts = vs.DotProducts[:lastOffset]
}

// AddUndefined adds the given number of quantized vectors to this set. The new
// quantized vector information should be set to defined values before use.
func (vs *RaBitQuantizedVectorSet) AddUndefined(count int) {
	newCount := len(vs.CodeCounts) + count
	vs.Codes.AddUndefined(count)
	vs.CodeCounts = slices.Grow(vs.CodeCounts, count)
	vs.CodeCounts = vs.CodeCounts[:newCount]
	vs.CentroidDistances = slices.Grow(vs.CentroidDistances, count)
	vs.CentroidDistances = vs.CentroidDistances[:newCount]
	vs.DotProducts = slices.Grow(vs.DotProducts, count)
	vs.DotProducts = vs.DotProducts[:newCount]
}
