package util

import "github.com/pingcap/tidb/mysql"

// ColumnInfo contains information of a column
type ColumnInfo struct {
	Schema             string
	Table              string
	OrgTable           string
	Name               string
	OrgName            string
	ColumnLength       uint32
	Charset            uint16
	Flag               uint16
	Decimal            uint8
	Type               uint8
	DefaultValueLength uint64
	DefaultValue       []byte
}

// Dump dumps ColumnInfo to bytes.
func (column *ColumnInfo) Dump(buffer []byte) []byte {
	buffer = DumpLengthEncodedString(buffer, []byte("def"))
	buffer = DumpLengthEncodedString(buffer, []byte(column.Schema))
	buffer = DumpLengthEncodedString(buffer, []byte(column.Table))
	buffer = DumpLengthEncodedString(buffer, []byte(column.OrgTable))
	buffer = DumpLengthEncodedString(buffer, []byte(column.Name))
	buffer = DumpLengthEncodedString(buffer, []byte(column.OrgName))

	buffer = append(buffer, 0x0c)

	buffer = DumpUint16(buffer, column.Charset)
	buffer = DumpUint32(buffer, column.ColumnLength)
	buffer = append(buffer, dumpType(column.Type))
	buffer = DumpUint16(buffer, dumpFlag(column.Type, column.Flag))
	buffer = append(buffer, column.Decimal)
	buffer = append(buffer, 0, 0)

	if column.DefaultValue != nil {
		buffer = DumpUint64(buffer, uint64(len(column.DefaultValue)))
		buffer = append(buffer, column.DefaultValue...)
	}

	return buffer
}

func DumpLengthEncodedString(buffer []byte, bytes []byte) []byte {
	buffer = DumpLengthEncodedInt(buffer, uint64(len(bytes)))
	buffer = append(buffer, bytes...)
	return buffer
}
func DumpUint16(buffer []byte, n uint16) []byte {
	buffer = append(buffer, byte(n))
	buffer = append(buffer, byte(n>>8))
	return buffer
}

func DumpUint32(buffer []byte, n uint32) []byte {
	buffer = append(buffer, byte(n))
	buffer = append(buffer, byte(n>>8))
	buffer = append(buffer, byte(n>>16))
	buffer = append(buffer, byte(n>>24))
	return buffer
}

func DumpUint64(buffer []byte, n uint64) []byte {
	buffer = append(buffer, byte(n))
	buffer = append(buffer, byte(n>>8))
	buffer = append(buffer, byte(n>>16))
	buffer = append(buffer, byte(n>>24))
	buffer = append(buffer, byte(n>>32))
	buffer = append(buffer, byte(n>>40))
	buffer = append(buffer, byte(n>>48))
	buffer = append(buffer, byte(n>>56))
	return buffer
}

func dumpFlag(tp byte, flag uint16) uint16 {
	switch tp {
	case mysql.TypeSet:
		return flag | uint16(mysql.SetFlag)
	case mysql.TypeEnum:
		return flag | uint16(mysql.EnumFlag)
	default:
		return flag
	}
}

func dumpType(tp byte) byte {
	switch tp {
	case mysql.TypeSet, mysql.TypeEnum:
		return mysql.TypeString
	default:
		return tp
	}
}

func DumpLengthEncodedInt(buffer []byte, n uint64) []byte {
	switch {
	case n <= 250:
		return append(buffer, TinyIntCache[n]...)

	case n <= 0xffff:
		return append(buffer, 0xfc, byte(n), byte(n>>8))

	case n <= 0xffffff:
		return append(buffer, 0xfd, byte(n), byte(n>>8), byte(n>>16))

	case n <= 0xffffffffffffffff:
		return append(buffer, 0xfe, byte(n), byte(n>>8), byte(n>>16), byte(n>>24),
			byte(n>>32), byte(n>>40), byte(n>>48), byte(n>>56))
	}

	return buffer
}

var TinyIntCache [251][]byte

func init() {
	for i := 0; i < len(TinyIntCache); i++ {
		TinyIntCache[i] = []byte{byte(i)}
	}
}
