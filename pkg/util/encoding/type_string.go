// Code generated by "stringer"; DO NOT EDIT.

package encoding

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[Unknown-0]
	_ = x[Null-1]
	_ = x[NotNull-2]
	_ = x[Int-3]
	_ = x[Float-4]
	_ = x[Decimal-5]
	_ = x[Bytes-6]
	_ = x[BytesDesc-7]
	_ = x[Time-8]
	_ = x[Duration-9]
	_ = x[True-10]
	_ = x[False-11]
	_ = x[UUID-12]
	_ = x[Array-13]
	_ = x[IPAddr-14]
	_ = x[SentinelType-15]
	_ = x[JSON-15]
	_ = x[Tuple-16]
	_ = x[BitArray-17]
	_ = x[BitArrayDesc-18]
	_ = x[TimeTZ-19]
	_ = x[Geo-20]
	_ = x[GeoDesc-21]
	_ = x[ArrayKeyAsc-22]
	_ = x[ArrayKeyDesc-23]
	_ = x[Box2D-24]
	_ = x[Void-25]
	_ = x[TSQuery-26]
	_ = x[TSVector-27]
}

func (i Type) String() string {
	switch i {
	case Unknown:
		return "Unknown"
	case Null:
		return "Null"
	case NotNull:
		return "NotNull"
	case Int:
		return "Int"
	case Float:
		return "Float"
	case Decimal:
		return "Decimal"
	case Bytes:
		return "Bytes"
	case BytesDesc:
		return "BytesDesc"
	case Time:
		return "Time"
	case Duration:
		return "Duration"
	case True:
		return "True"
	case False:
		return "False"
	case UUID:
		return "UUID"
	case Array:
		return "Array"
	case IPAddr:
		return "IPAddr"
	case SentinelType:
		return "SentinelType"
	case Tuple:
		return "Tuple"
	case BitArray:
		return "BitArray"
	case BitArrayDesc:
		return "BitArrayDesc"
	case TimeTZ:
		return "TimeTZ"
	case Geo:
		return "Geo"
	case GeoDesc:
		return "GeoDesc"
	case ArrayKeyAsc:
		return "ArrayKeyAsc"
	case ArrayKeyDesc:
		return "ArrayKeyDesc"
	case Box2D:
		return "Box2D"
	case Void:
		return "Void"
	case TSQuery:
		return "TSQuery"
	case TSVector:
		return "TSVector"
	default:
		return "Type(" + strconv.FormatInt(int64(i), 10) + ")"
	}
}
