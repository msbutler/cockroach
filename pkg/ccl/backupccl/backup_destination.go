// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"io/ioutil"
	"net/url"
	"path"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// fetchPreviousBackups takes a list of URIs of previous backups and returns
// their manifest as well as the encryption options of the first backup in the
// chain.
func fetchPreviousBackups(
	ctx context.Context,
	user security.SQLUsername,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	prevBackupURIs []string,
	encryptionParams backupEncryptionParams,
) ([]BackupManifest, *jobspb.BackupEncryptionOptions, error) {
	if len(prevBackupURIs) == 0 {
		return nil, nil, nil
	}

	baseBackup := prevBackupURIs[0]
	encryptionOptions, err := getEncryptionFromBase(ctx, user, makeCloudStorage, baseBackup,
		encryptionParams)
	if err != nil {
		return nil, nil, err
	}
	prevBackups, err := getBackupManifests(ctx, user, makeCloudStorage, prevBackupURIs,
		encryptionOptions)
	if err != nil {
		return nil, nil, err
	}

	return prevBackups, encryptionOptions, nil
}

type backupDestination struct {
	//defaultURI is the full path of the planned backup
	defaultURI string

	//collectionURI is the path to the collection, or nil if the backup is not
	//apart of a collection
	collectionURI    string
	chosenSubdir     string
	urisByLocalityKV map[string]string
}

// resolveDest resolves the true destination of a backup. The backup command
// provided by the user may point to a backup collection, or a backup location
// which auto-appends incremental backups to it. This method checks for these
// cases and finds the actual directory where we'll write this new backup.
//
// In addition, in this case that this backup is an incremental backup (either
// explicitly, or due to the auto-append feature), it will resolve the
// encryption options based on the base backup, as well as find all previous
// backup manifests in the backup chain.
func resolveDest(
	ctx context.Context,
	user security.SQLUsername,
	nested, appendToLatest bool,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	endTime hlc.Timestamp,
	to []string,
	incrementalFrom []string,
	subdir string,
	incrementalStorage []string,
) (
	*backupDestination,
	[]string, /* prevBackupURIs - list of full paths for previous backups in the chain */
	error,
) {
	var prevBackupURIs []string
	var err error

<<<<<<< Updated upstream
=======
	defaultURI, urisByLocalityKV, err := getURIsByLocalityKV(to, "")
	if err != nil {
		return nil, nil, err
	}

	backupDest := new(backupDestination)

>>>>>>> Stashed changes
	if nested {
		backupDest.collectionURI, backupDest.chosenSubdir, err =
			resolveBackupCollection(ctx, user, defaultURI,
				appendToLatest, makeCloudStorage, endTime, subdir)
		if err != nil {
			return nil, nil, err
		}
<<<<<<< Updated upstream
		defaultURI, urisByLocalityKV, err = getURIsByLocalityKV(to, chosenSuffix)
=======
		defaultURI, urisByLocalityKV, err = getURIsByLocalityKV(to, backupDest.chosenSubdir)
>>>>>>> Stashed changes
		if err != nil {
			return nil, nil, err
		}
	}

<<<<<<< Updated upstream
	// At this point, the defaultURI is the full path for the backup. For BACKUP
	// INTO, this path includes the chosenSuffix Once this function returns, the
	// defaultURI will be the full path for this backup in planning.
=======
	// At this point, defaultURI is the path for the full backup we plan to take
	// or increment on. Once this function returns, the defaultURI will be the
	// full path for this backup in planning.
>>>>>>> Stashed changes
	if len(incrementalFrom) != 0 {
		prevBackupURIs = incrementalFrom
	} else {
		defaultStore, err := makeCloudStorage(ctx, defaultURI, user)
		if err != nil {
			return nil, nil, err
		}
		defer defaultStore.Close()
		exists, err := containsManifest(ctx, defaultStore)
		if err != nil {
			return nil, nil, err
		}
		if exists {
			// The defaultStore contains a full backup; consequently,
			// we're conducting an incremental backup.
			prevBackupURIs = append(prevBackupURIs, defaultURI)

			var priors []string
			var backupChainURI string
			if len(incrementalStorage) > 0 {
				// Implies the incremental backup chain lives in
<<<<<<< Updated upstream
				// incrementalStorage/chosenSuffix, while the full backup lives in
				// defaultStore/chosenSuffix. The incremental backup chain in
				// incrementalStorage/chosenSuffix will not contain any incremental backups that live
				// elsewhere.
				backupChainURI, _, err = getURIsByLocalityKV(incrementalStorage,
					chosenSuffix)
=======
				// incrementalStorage/chosenSubdir, while the full backup lives in
				// defaultStore/chosenSubdir.
				_, backupChainURI, err = getLocalityAndBaseURI(incrementalStorage[0],
					backupDest.chosenSubdir)
>>>>>>> Stashed changes
				if err != nil {
					return nil, nil, err
				}
				incrementalStore, err := makeCloudStorage(ctx, backupChainURI, user)
				if err != nil {
					return nil, nil, err
				}
				defer incrementalStore.Close()

				priors, err = FindPriorBackups(ctx, incrementalStore, OmitManifest)
				if err != nil {
					return nil, nil, err
				}
			} else {
				backupChainURI = defaultURI
				priors, err = FindPriorBackups(ctx, defaultStore, OmitManifest)
				if err != nil {
					return nil, nil, err
				}
			}
			for _, prior := range priors {
				priorURI, err := url.Parse(backupChainURI)
				if err != nil {
					return nil, nil, errors.Wrapf(err, "parsing default backup location %s",
						backupChainURI)
				}
				priorURI.Path = path.Join(priorURI.Path, prior)
				prevBackupURIs = append(prevBackupURIs, priorURI.String())
			}
			if err != nil {
				return nil, nil, errors.Wrap(err, "finding previous backups")
			}

			// Within the chosenSubdir dir, differentiate files with partName.
			partName := endTime.GoTime().Format(DateBasedIncFolderName)
			partName = path.Join(backupDest.chosenSubdir, partName)
			var prefix []string
			if len(incrementalStorage) > 0 {
				prefix = incrementalStorage
			} else {
				prefix = to
			}
			defaultURI, urisByLocalityKV, err = getURIsByLocalityKV(prefix, partName)
			if err != nil {
				return nil, nil, errors.Wrap(err, "adjusting backup destination to append new layer to existing backup")
			}
		}
	}
	backupDest.defaultURI = defaultURI
	backupDest.urisByLocalityKV = urisByLocalityKV
	return backupDest, prevBackupURIs, nil
}

// getBackupManifests fetches the backup manifest from a list of backup URIs.
func getBackupManifests(
	ctx context.Context,
	user security.SQLUsername,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	backupURIs []string,
	encryption *jobspb.BackupEncryptionOptions,
) ([]BackupManifest, error) {
	manifests := make([]BackupManifest, len(backupURIs))
	if len(backupURIs) == 0 {
		return manifests, nil
	}

	g := ctxgroup.WithContext(ctx)
	for i := range backupURIs {
		i := i
		g.GoCtx(func(ctx context.Context) error {
			// TODO(lucy): We may want to upgrade the table descs to the newer
			// foreign key representation here, in case there are backups from an
			// older cluster. Keeping the descriptors as they are works for now
			// since all we need to do is get the past backups' table/index spans,
			// but it will be safer for future code to avoid having older-style
			// descriptors around.
			uri := backupURIs[i]
			desc, err := ReadBackupManifestFromURI(
				ctx, uri, user, makeCloudStorage, encryption,
			)
			if err != nil {
				return errors.Wrapf(err, "failed to read backup from %q",
					RedactURIForErrorMessage(uri))
			}
			manifests[i] = desc
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	return manifests, nil
}

// getEncryptionFromBase retrieves the encryption options of a base backup. It
// is expected that incremental backups use the same encryption options as the
// base backups.
func getEncryptionFromBase(
	ctx context.Context,
	user security.SQLUsername,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	baseBackupURI string,
	encryptionParams backupEncryptionParams,
) (*jobspb.BackupEncryptionOptions, error) {
	var encryptionOptions *jobspb.BackupEncryptionOptions
	if encryptionParams.encryptMode != noEncryption {
		exportStore, err := makeCloudStorage(ctx, baseBackupURI, user)
		if err != nil {
			return nil, err
		}
		defer exportStore.Close()
		opts, err := readEncryptionOptions(ctx, exportStore)
		if err != nil {
			return nil, err
		}

		switch encryptionParams.encryptMode {
		case passphrase:
			encryptionOptions = &jobspb.BackupEncryptionOptions{
				Mode: jobspb.EncryptionMode_Passphrase,
				Key:  storageccl.GenerateKey(encryptionParams.encryptionPassphrase, opts.Salt),
			}
		case kms:
			defaultKMSInfo, err := validateKMSURIsAgainstFullBackup(encryptionParams.kmsURIs,
				newEncryptedDataKeyMapFromProtoMap(opts.EncryptedDataKeyByKMSMasterKeyID), encryptionParams.kmsEnv)
			if err != nil {
				return nil, err
			}
			encryptionOptions = &jobspb.BackupEncryptionOptions{
				Mode:    jobspb.EncryptionMode_KMS,
				KMSInfo: defaultKMSInfo}
		}
	}
	return encryptionOptions, nil
}

// resolveBackupCollection returns the collectionURI and chosenSuffix that we
// should use for a backup that is pointing to a collection.
func resolveBackupCollection(
	ctx context.Context,
	user security.SQLUsername,
	defaultURI string,
	appendToLatest bool,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	endTime hlc.Timestamp,
	subdir string,
) (string, string, error) {
	var chosenSubdir string
	collectionURI := defaultURI

	if appendToLatest {
		// User called 'BACKUP ... INTO LATEST IN ...', i.e.appendToLatest == True,
		latest, err := readLatestFile(ctx, collectionURI, makeCloudStorage, user)
		if err != nil {
			return "", "", err
		}
		chosenSubdir = latest
	} else if subdir != "" {
		// User has specified a subdir via `BACKUP INTO 'subdir' IN...`.
		chosenSubdir = strings.TrimPrefix(subdir, "/")
		chosenSubdir = "/" + chosenSubdir
	} else {
		chosenSubdir = endTime.GoTime().Format(DateBasedIntoFolderName)
	}
	return collectionURI, chosenSubdir, nil
}

func readLatestFile(
	ctx context.Context,
	collectionURI string,
	makeCloudStorage cloud.ExternalStorageFromURIFactory,
	user security.SQLUsername,
) (string, error) {
	collection, err := makeCloudStorage(ctx, collectionURI, user)
	if err != nil {
		return "", err
	}
	defer collection.Close()
	latestFile, err := collection.ReadFile(ctx, latestFileName)
	if err != nil {
		if errors.Is(err, cloud.ErrFileDoesNotExist) {
			return "", pgerror.Wrapf(err, pgcode.UndefinedFile, "path does not contain a completed latest backup")
		}
		return "", pgerror.WithCandidateCode(err, pgcode.Io)
	}
	latest, err := ioutil.ReadAll(latestFile)
	if err != nil {
		return "", err
	}
	if len(latest) == 0 {
		return "", errors.Errorf("malformed LATEST file")
	}
	return string(latest), nil
}
