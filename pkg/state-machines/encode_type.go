package machines

import "encoding/binary"

// EncodeRaftType marks the last byte in the buffer with it's type info.
func EncodeRaftType(rtype uint16, buf []byte) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, rtype)
	buf = append(buf, b...)
	return buf
}
