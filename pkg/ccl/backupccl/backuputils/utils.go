// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backuputils

import (
	"context"
	"net/url"
	"path"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/doctor"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// URLSeparator represents the standard separator used in backup URLs.
const URLSeparator = '/'

// RedactURIForErrorMessage redacts any storage secrets before returning a URI which is safe to
// return to the client in an error message.
func RedactURIForErrorMessage(uri string) string {
	redactedURI, err := cloud.SanitizeExternalStorageURI(uri, []string{})
	if err != nil {
		return "<uri_failed_to_redact>"
	}
	return redactedURI
}

// JoinURLPath forces a relative path join by removing any leading slash, then
// re-prepending it later.
//
// Stores are an odd combination of absolute and relative path.
// They present as absolute paths, since they contain a hostname. URL.Parse
// thus prepends each URL.Path with a leading slash.
// But some schemes, e.g. nodelocal, can legally travel _above_ the ostensible
// root (e.g. nodelocal://0/.../). This is not typically possible in file
// paths, and the standard path package doesn't like it. Specifically, it will
// clean up something like nodelocal://0/../ to nodelocal://0. This is normally
// correct behavior, but is wrong here.
//
// In point of fact we block this URLs resolved this way elsewhere. But we
// still want to make sure to resolve the paths correctly here. We don't want
// to accidentally correct an unauthorized file path to an authorized one, then
// write a backup to an unexpected place or print the wrong error message on
// a restore.
func JoinURLPath(args ...string) string {
	argsCopy := make([]string, 0)
	for _, arg := range args {
		if len(arg) == 0 {
			continue
		}
		// We only want non-empty tokens.
		argsCopy = append(argsCopy, arg)
	}
	if len(argsCopy) == 0 {
		return path.Join(argsCopy...)
	}

	// We have at least 1 arg, and each has at least length 1.
	isAbs := false
	if argsCopy[0][0] == URLSeparator {
		isAbs = true
		argsCopy[0] = argsCopy[0][1:]
	}
	joined := path.Join(argsCopy...)
	if isAbs {
		joined = string(URLSeparator) + joined
	}
	return joined
}

// AppendPaths appends the tailDir to the `path` of the passed in uris.
func AppendPaths(uris []string, tailDir ...string) ([]string, error) {
	retval := make([]string, len(uris))
	for i, uri := range uris {
		parsed, err := url.Parse(uri)
		if err != nil {
			return nil, err
		}
		joinArgs := append([]string{parsed.Path}, tailDir...)
		parsed.Path = JoinURLPath(joinArgs...)
		retval[i] = parsed.String()
	}
	return retval, nil
}

// ValidateDescriptors ensures that the passed in descriptors are in a valid
// state. The first returned parameter is true if all descriptors are valid;
// the second provides a description of any descriptors in an invalid state; the
// third returns an error if the descriptors could not be properly validated.
// Note that this function does not check any references to the jobs table in
// the descriptors.
func ValidateDescriptors(
	ctx context.Context, descriptors catalog.Descriptors, version roachpb.Version,
) (bool, string, error) {
	descTable := make(doctor.DescriptorTable, 0, len(descriptors))
	namespaceTable := make(doctor.NamespaceTable, 0, len(descriptors))

	for _, desc := range descriptors {
		bytes, err := protoutil.Marshal(desc.DescriptorProto())
		if err != nil {
			return false, "", err
		}
		descTable = append(descTable,
			doctor.DescriptorTableRow{
				ID:        int64(desc.GetID()),
				DescBytes: bytes,
				ModTime:   desc.GetModificationTime(),
			})
		if !desc.Dropped() {
			// Descriptors in the process of getting dropped do not appear in the namespace table.
			namespaceTable = append(namespaceTable,
				doctor.NamespaceTableRow{
					ID: int64(desc.GetID()),
					NameInfo: descpb.NameInfo{
						Name:           desc.GetName(),
						ParentID:       desc.GetParentID(),
						ParentSchemaID: desc.GetParentSchemaID(),
					},
				})
		}
	}
	validationMessages := strings.Builder{}
	// Don't validate any jobs, since
	// these will be synthesized by the restore process.
	ok, err := doctor.Examine(ctx,
		clusterversion.ClusterVersion{Version: version},
		descTable, namespaceTable,
		nil,
		false, /*validateJobs*/
		false,
		&validationMessages)
	if err != nil {
		return false, "", err

	}
	return ok, validationMessages.String(), nil
}
