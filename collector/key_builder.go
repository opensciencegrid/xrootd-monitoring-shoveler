package collector

import (
	"strconv"
	"sync"

	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
)

// KeyBuilder provides efficient string key building with minimal allocations
// Uses a pool of byte buffers to avoid repeated allocations
type KeyBuilder struct {
	buf []byte
}

var keyBuilderPool = sync.Pool{
	New: func() interface{} {
		return &KeyBuilder{
			buf: make([]byte, 0, 128), // Pre-allocate reasonable size
		}
	},
}

// GetKeyBuilder retrieves a KeyBuilder from the pool
func GetKeyBuilder() *KeyBuilder {
	return keyBuilderPool.Get().(*KeyBuilder)
}

// Release returns the KeyBuilder to the pool for reuse
func (kb *KeyBuilder) Release() {
	kb.buf = kb.buf[:0] // Reset but keep capacity
	keyBuilderPool.Put(kb)
}

// String returns the built string
func (kb *KeyBuilder) String() string {
	return string(kb.buf)
}

// Reset clears the buffer for reuse within same KeyBuilder instance
func (kb *KeyBuilder) Reset() {
	kb.buf = kb.buf[:0]
}

// WriteByte writes a single byte
func (kb *KeyBuilder) WriteByte(b byte) error {
	kb.buf = append(kb.buf, b)
	return nil
}

// WriteString writes a string
func (kb *KeyBuilder) WriteString(s string) {
	kb.buf = append(kb.buf, s...)
}

// WriteBytes writes a byte slice
func (kb *KeyBuilder) WriteBytes(b []byte) {
	kb.buf = append(kb.buf, b...)
}

// WriteUint32 writes a uint32 in decimal format
func (kb *KeyBuilder) WriteUint32(n uint32) {
	kb.buf = strconv.AppendUint(kb.buf, uint64(n), 10)
}

// WriteInt64 writes an int64 in decimal format
func (kb *KeyBuilder) WriteInt64(n int64) {
	kb.buf = strconv.AppendInt(kb.buf, n, 10)
}

// WriteInt32 writes an int32 in hexadecimal format
func (kb *KeyBuilder) WriteInt32Hex(n int32) {
	kb.buf = strconv.AppendInt(kb.buf, int64(n), 16)
}

// BuildDictKey builds a dictionary key: serverID-dict-dictID
func BuildDictKey(serverID string, dictID uint32) string {
	kb := GetKeyBuilder()
	defer kb.Release()

	kb.WriteString(serverID)
	kb.WriteString("-dict-")
	kb.WriteUint32(dictID)

	return kb.String()
}

// BuildDictIDKey builds a dict ID key: serverID-dictid-dictID
func BuildDictIDKey(serverID string, dictID uint32) string {
	kb := GetKeyBuilder()
	defer kb.Release()

	kb.WriteString(serverID)
	kb.WriteString("-dictid-")
	kb.WriteUint32(dictID)

	return kb.String()
}

// BuildFileKey builds a file key: serverID-file-fileID
func BuildFileKey(serverID string, fileID uint32) string {
	kb := GetKeyBuilder()
	defer kb.Release()

	kb.WriteString(serverID)
	kb.WriteString("-file-")
	kb.WriteUint32(fileID)

	return kb.String()
}

// BuildTimeKey builds a time key: serverID-time-fileID-sid
func BuildTimeKey(serverID string, fileID uint32, sid int64) string {
	kb := GetKeyBuilder()
	defer kb.Release()

	kb.WriteString(serverID)
	kb.WriteString("-time-")
	kb.WriteUint32(fileID)
	_ = kb.WriteByte('-') // Error is always nil
	kb.WriteInt64(sid)

	return kb.String()
}

// BuildUserInfoKey builds a user info key: serverID-userinfo-protocol/username.pid:sid@host
func BuildUserInfoKey(serverID string, info parser.UserInfo) string {
	kb := GetKeyBuilder()
	defer kb.Release()

	kb.WriteString(serverID)
	kb.WriteString("-userinfo-")
	kb.WriteString(info.Protocol)
	_ = kb.WriteByte('/') // Error is always nil
	kb.WriteString(info.Username)
	_ = kb.WriteByte('.') // Error is always nil
	kb.WriteUint32(uint32(info.Pid))
	_ = kb.WriteByte(':') // Error is always nil
	kb.WriteUint32(uint32(info.Sid))
	_ = kb.WriteByte('@') // Error is always nil
	kb.WriteString(info.Host)

	return kb.String()
}

// BuildServerID builds a server ID: serverStart#remoteAddr
func BuildServerID(serverStart int32, remoteAddr string) string {
	kb := GetKeyBuilder()
	defer kb.Release()

	kb.buf = strconv.AppendInt(kb.buf, int64(serverStart), 10)
	_ = kb.WriteByte('#') // Error is always nil
	kb.WriteString(remoteAddr)

	return kb.String()
}

// BuildUserHex builds a hex user string from userID
func BuildUserHex(userID uint32) string {
	kb := GetKeyBuilder()
	defer kb.Release()

	kb.WriteInt32Hex(int32(userID))

	return kb.String()
}
