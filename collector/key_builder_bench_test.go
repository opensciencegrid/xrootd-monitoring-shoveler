package collector

import (
	"fmt"
	"testing"

	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
)

// Benchmark comparing old fmt.Sprintf approach vs new KeyBuilder approach

func BenchmarkKeyGeneration(b *testing.B) {
	serverID := "12345#192.168.1.100:1094"
	dictID := uint32(54321)
	fileID := uint32(99999)

	b.Run("fmt.Sprintf-DictKey", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = fmt.Sprintf("%s-dict-%d", serverID, dictID)
		}
	})

	b.Run("KeyBuilder-DictKey", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = BuildDictKey(serverID, dictID)
		}
	})

	b.Run("fmt.Sprintf-FileKey", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = fmt.Sprintf("%s-file-%d", serverID, fileID)
		}
	})

	b.Run("KeyBuilder-FileKey", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = BuildFileKey(serverID, fileID)
		}
	})

	b.Run("fmt.Sprintf-TimeKey", func(b *testing.B) {
		b.ReportAllocs()
		sid := int64(123456789)
		for i := 0; i < b.N; i++ {
			_ = fmt.Sprintf("%s-time-%d-%d", serverID, fileID, sid)
		}
	})

	b.Run("KeyBuilder-TimeKey", func(b *testing.B) {
		b.ReportAllocs()
		sid := int64(123456789)
		for i := 0; i < b.N; i++ {
			_ = BuildTimeKey(serverID, fileID, sid)
		}
	})
}

func BenchmarkUserInfoString(b *testing.B) {
	userInfo := parser.UserInfo{
		Protocol: "xrootd",
		Username: "testuser",
		Pid:      12345,
		Sid:      67890,
		Host:     "client.example.com",
	}
	serverID := "12345#192.168.1.100:1094"

	b.Run("fmt.Sprintf-UserInfo", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = fmt.Sprintf("%s/%s.%d:%d@%s", userInfo.Protocol, userInfo.Username,
				userInfo.Pid, userInfo.Sid, userInfo.Host)
		}
	})

	b.Run("KeyBuilder-UserInfoPartial", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			kb := GetKeyBuilder()
			kb.WriteString(userInfo.Protocol)
			kb.WriteByte('/')
			kb.WriteString(userInfo.Username)
			kb.WriteByte('.')
			kb.WriteUint32(uint32(userInfo.Pid))
			kb.WriteByte(':')
			kb.WriteUint32(uint32(userInfo.Sid))
			kb.WriteByte('@')
			kb.WriteString(userInfo.Host)
			_ = kb.String()
			kb.Release()
		}
	})

	b.Run("fmt.Sprintf-UserInfoKey", func(b *testing.B) {
		b.ReportAllocs()
		userInfoStr := fmt.Sprintf("%s/%s.%d:%d@%s", userInfo.Protocol, userInfo.Username,
			userInfo.Pid, userInfo.Sid, userInfo.Host)
		for i := 0; i < b.N; i++ {
			_ = fmt.Sprintf("%s-userinfo-%s", serverID, userInfoStr)
		}
	})

	b.Run("KeyBuilder-UserInfoKey", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = BuildUserInfoKey(serverID, userInfo)
		}
	})
}

func BenchmarkServerIDGeneration(b *testing.B) {
	serverStart := int32(1234567890)
	remoteAddr := "192.168.1.100:1094"

	b.Run("fmt.Sprintf-ServerID", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = fmt.Sprintf("%d#%s", serverStart, remoteAddr)
		}
	})

	b.Run("KeyBuilder-ServerID", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = BuildServerID(serverStart, remoteAddr)
		}
	})
}

// Benchmark multiple key generations in sequence (more realistic workload)
func BenchmarkMultipleKeys(b *testing.B) {
	serverID := "12345#192.168.1.100:1094"
	dictID := uint32(54321)
	fileID := uint32(99999)
	userInfo := parser.UserInfo{
		Protocol: "xrootd",
		Username: "testuser",
		Pid:      12345,
		Sid:      67890,
		Host:     "client.example.com",
	}

	b.Run("fmt.Sprintf-Multiple", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = fmt.Sprintf("%s-dict-%d", serverID, dictID)
			_ = fmt.Sprintf("%s-dictid-%d", serverID, dictID)
			_ = fmt.Sprintf("%s-file-%d", serverID, fileID)
			userInfoStr := fmt.Sprintf("%s/%s.%d:%d@%s", userInfo.Protocol,
				userInfo.Username, userInfo.Pid, userInfo.Sid, userInfo.Host)
			_ = fmt.Sprintf("%s-userinfo-%s", serverID, userInfoStr)
		}
	})

	b.Run("KeyBuilder-Multiple", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = BuildDictKey(serverID, dictID)
			_ = BuildDictIDKey(serverID, dictID)
			_ = BuildFileKey(serverID, fileID)
			_ = BuildUserInfoKey(serverID, userInfo)
		}
	})
}
