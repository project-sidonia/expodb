package machines

import "encoding/binary"

// EncodeRaftType marks the last byte in the buffer with it's type info.
func EncodeRaftType(rtype uint16, buf []byte) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, rtype)
	buf = append(buf, b...)
	return buf
}

func DecodeRaftType(buf []byte) ([]byte, uint16) {
	return buf[0 : len(buf)-2], binary.BigEndian.Uint16(buf[len(buf)-2:])
}
