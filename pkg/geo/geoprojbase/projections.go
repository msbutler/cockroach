// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This file was generated from `./pkg/cmd/generate-spatial-ref-sys`.

package geoprojbase

import (
	_ "embed" // required for go:embed
	"sync"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/errors"
)

//go:embed data/proj.json.gz
var projData []byte

var once sync.Once
var projectionsInternal map[geopb.SRID]ProjInfo

// MakeSpheroid is an injectable function which creates a spheroid.
// If you hit the assertion here, you may want to blank import geographic lib, e.g.
// _ "github.com/cockroachdb/cockroach/pkg/geo/geographiclib".
var MakeSpheroid = func(radius, flattening float64) (Spheroid, error) {
	return nil, errors.AssertionFailedf("MakeSpheroid not initialised")
}

// getProjections returns the mapping of SRID to projections.
// Use the `Projection` function to obtain one.
func getProjections() map[geopb.SRID]ProjInfo {

	return nil
}
