// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/xitongsys/parquet-go-source/writerfile"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/tool/parquet-tools/schematool"
	"github.com/xitongsys/parquet-go/writer"
	"log"
	"strings"
	"encoding/json"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	_ "github.com/xitongsys/parquet-go/parquet"
	_ "github.com/xitongsys/parquet-go/tool/parquet-tools/schematool"
	_ "github.com/xitongsys/parquet-go/writer"
	_ "github.com/xitongsys/parquet-go-source/writerfile"

)

//const exportFilePatternPart = "%part%"
//const exportFilePatternDefault = exportFilePatternPart + ".csv"

// csvExporter data structure to augment the compression
// and csv writer, encapsulating the internals to make
// exporting oblivious for the consumers.
type parquetExporter struct {
	compressor *gzip.Writer
	buf        *bytes.Buffer
	parquetWriter  *writer.ParquetWriter
}

// Write append record to parquet file.
func (c *parquetExporter) Write(record map[string]string) error { // NOT QUITE, TO INTERFACE,
	// MAKE INPUT AN ARRAY
	recordJSON, err := json.Marshal(record)
	if err != nil{
		return err
	}
	return c.parquetWriter.Write(recordJSON)
}

// Write append record to parquet file.
func (c *parquetExporter) Flush() error {
	return c.parquetWriter.Flush(false) // NEED TO FIGURE OUT WHAT THIS FLAG DOES!
}

// Close closes the parquet writer
func (c *parquetExporter) Close() error {
	err := c.parquetWriter.WriteStop()
	//if c.compressor != nil {
	//	return c.compressor.Close()
	//}
	return err
}

// Bytes results in the slice of bytes with compressed content.
func (c *parquetExporter) Bytes() []byte { // IDENTICAL
	return c.buf.Bytes()
}

func (c *parquetExporter) ResetBuffer(){ // IDENTICAL
	c.buf.Reset()
}

// Len returns length of the buffer with content. //IDENTICAL
func (c *parquetExporter) Len() int {
	return c.buf.Len()
}

func (c *parquetExporter) FileName(spec execinfrapb.ParquetWriterSpec,
	part string) string { // DIFFERENT INPU
	pattern := exportFilePatternDefault
	if spec.NamePattern != "" {
		pattern = spec.NamePattern
	}

	fileName := strings.Replace(pattern, exportFilePatternPart, part, -1)
	if c.compressor != nil {
		fileName += ".gz"
	}
	return fileName
}

func newParquetExporter(sp execinfrapb.ParquetWriterSpec) (*parquetExporter, error){

	schema, err := newParquetSchema()
	if err !=nil{
		return nil, err
	}

	var exporter *parquetExporter

	buf := bytes.NewBuffer([]byte{})
	wf := writerfile.NewWriterFile(buf)
	pw, err := writer.NewParquetWriter(wf,schema,4)

	if err != nil{
		return nil, err
	}

	switch sp.CompressionCodec {
	default:
		{
			exporter = &parquetExporter{
				buf:       buf,
				parquetWriter: pw,
			}
		}
	}

	return exporter, nil
}

type parquetToJSON struct{
	Tag string `json:"String"`
	Fields []parquetToJSON `json:"Fields,omitempty"`
}

// create JSON schema for parquet file
func newParquetSchema() ([]byte, error){

	root := new(parquetToJSON)
	root.Tag = "name=root, repetitiontype=REQUIRED"

	colSchema:= make([]parquetToJSON, len(colinfo.ExportColumns))

	for i := 0; i< len(colinfo.ExportColumns); i++{
		colType := typeToParquet(colinfo.ExportColumns[i].Typ)
		colSchema[i].Tag = fmt.Sprintf("name=%s,inname=%s,type=%s,repetitiontype=REQUIRED",
			colinfo.ExportColumns[i].Name, strings.ToUpper(colinfo.ExportColumns[i].Name), colType)
	}
	root.Fields = colSchema

	return json.Marshal(root)
}

func typeToParquet(typ *types.T) string{
	var parquetType parquet.Type
	switch typ{
	case types.Bool:
		parquetType = parquet.Type_BOOLEAN
	case types.String:
		parquetType = parquet.Type_BYTE_ARRAY
	case types.Int4:
		parquetType = parquet.Type_INT32

	// everything else, for now...
	default:
		parquetType = parquet.Type_BYTE_ARRAY
	}
	parquetTypeString, _ := schematool.ParquetTypeToParquetTypeStr(&parquetType,nil)
	return parquetTypeString
}

func newParquetWriterProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.ParquetWriterSpec,
	input execinfra.RowSource,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	c := &parquetWriterProcessor{
		flowCtx:     flowCtx,
		processorID: processorID,
		spec:        spec,
		input:       input,
		output:      output,
	}
	semaCtx := tree.MakeSemaContext()
	if err := c.out.Init(&execinfrapb.PostProcessSpec{}, c.OutputTypes(), &semaCtx, flowCtx.NewEvalCtx()); err != nil {
		return nil, err
	}
	return c, nil
}



type parquetWriterProcessor struct {
	flowCtx     *execinfra.FlowCtx
	processorID int32
	spec        execinfrapb.ParquetWriterSpec
	input       execinfra.RowSource
	out         execinfra.ProcOutputHelper
	output      execinfra.RowReceiver
}

var _ execinfra.Processor = &parquetWriterProcessor{}

func (sp *parquetWriterProcessor) OutputTypes() []*types.T {
	res := make([]*types.T, len(colinfo.ExportColumns))
	for i := range res {
		res[i] = colinfo.ExportColumns[i].Typ
	}
	return res
}

func (sp *parquetWriterProcessor) MustBeStreaming() bool {
	return false
}

func (sp *parquetWriterProcessor) Run(ctx context.Context) {
	ctx, span := tracing.ChildSpan(ctx, "csvWriter")
	defer span.Finish()

	instanceID := sp.flowCtx.EvalCtx.NodeID.SQLInstanceID()
	uniqueID := builtins.GenerateUniqueInt(instanceID)

	err := func() error {

		sp.input.Start(ctx)
		input := execinfra.MakeNoMetadataRowSource(sp.input, sp.output)

		alloc := &rowenc.DatumAlloc{}
		typs := sp.input.OutputTypes()

		writer, err := newParquetExporter(sp.spec) // diff func name from csv, but could be same
		if err != nil {
			return err
		}

		var nullsAs string
		if sp.spec.Options.NullEncoding != nil {
			nullsAs = *sp.spec.Options.NullEncoding
		}

		f := tree.NewFmtCtx(tree.FmtExport)
		defer f.Close()

		parquetRow := make(map[string]string,len(colinfo.ExportColumns))

		chunk := 0
		done := false
		for {
			var rows int64
			writer.ResetBuffer()
			for {
				// If the bytes.Buffer sink exceeds the target size of a Parquet file, we
				// flush before exporting any additional rows.
				if int64(writer.buf.Len()) >= sp.spec.ChunkSize {
					break
				}
				if sp.spec.ChunkRows > 0 && rows >= sp.spec.ChunkRows {
					break
				}
				row, err := input.NextRow()
				if err != nil {
					return err
				}
				if row == nil {
					done = true
					break
				}
				rows++

				for i, ed := range row {
					if ed.IsNull() {
						if sp.spec.Options.NullEncoding != nil {
							parquetRow[colinfo.ExportColumns[i].Name] = nullsAs
							continue
						} else {
							return errors.New("NULL value encountered during EXPORT, " +
								"use `WITH nullas` to specify the string representation of NULL")
						}
					}
					if err := ed.EnsureDecoded(typs[i], alloc); err != nil {
						return err
					}
					ed.Datum.Format(f)
					parquetRow[colinfo.ExportColumns[i].Name] = f.String()
					f.Reset()
				}
				// this write implementation uses a map, instead of an array :(
				if err := writer.Write(parquetRow); err != nil {
					return err
				}
			}
			if rows < 1 {
				break
			}
			if err := writer.Flush(); err != nil {
				return errors.Wrap(err, "failed to flush parquet writer")
			}

			conf, err := cloud.ExternalStorageConfFromURI(sp.spec.Destination, sp.spec.User())
			if err != nil {
				return err
			}
			es, err := sp.flowCtx.Cfg.ExternalStorage(ctx, conf)
			if err != nil {
				return err
			}
			defer es.Close()

			part := fmt.Sprintf("n%d.%d", uniqueID, chunk)
			chunk++
			filename := writer.FileName(sp.spec, part)
			// Close writer to ensure buffer and any compression footer is flushed.
			err = writer.Close()
			if err != nil {
				return errors.Wrapf(err, "failed to close exporting writer")
			}

			size := writer.Len()

			if err := cloud.WriteFile(ctx, es, filename, bytes.NewReader(writer.Bytes())); err != nil {
				return err
			}
			res := rowenc.EncDatumRow{
				rowenc.DatumToEncDatum(
					types.String,
					tree.NewDString(filename),
				),
				rowenc.DatumToEncDatum(
					types.Int,
					tree.NewDInt(tree.DInt(rows)),
				),
				rowenc.DatumToEncDatum(
					types.Int,
					tree.NewDInt(tree.DInt(size)),
				),
			}

			cs, err := sp.out.EmitRow(ctx, res, sp.output)
			if err != nil {
				return err
			}
			if cs != execinfra.NeedMoreRows {
				// TODO(dt): presumably this is because our recv already closed due to
				// another error... so do we really need another one?
				return errors.New("unexpected closure of consumer")
			}
			if done {
				break
			}
		}

		return nil
	}()

	// TODO(dt): pick up tracing info in trailing meta
	execinfra.DrainAndClose(
		ctx, sp.output, err, func(context.Context) {} /* pushTrailingMeta */, sp.input)
}

func init() {
	rowexec.NewParquetWriterProcessor = newParquetWriterProcessor
}
