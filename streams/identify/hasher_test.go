package identify

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/testutils"
	"github.com/stretchr/testify/require"
)

// This code can be used for hash benchmarking

// bcf8e3de41ebc23d88a0bfeb24402fca74048f7bc6ab79e0cf5c86552d17566d - 4mb
// 1f0eeaa50f456887c306868ca7edf09cfecfd0d0529243a13c9cff22e32d7d15 - 500mb

func BenchmarkHashSpeedTiny(t *testing.B) {
	activeFileManager, err := testutils.NewFileManager()
	require.Nil(t, err)
	rawBytes, err := activeFileManager.DownloadFileBytes("bcf8e3de41ebc23d88a0bfeb24402fca74048f7bc6ab79e0cf5c86552d17566d")
	if err != nil {
		return
	}
	file, err := os.CreateTemp("", "test-hasher")
	require.Nil(t, err)
	defer file.Close()
	defer os.Remove(file.Name())

	// Write whole file to disk.
	file.Write(rawBytes)
	_, err = file.Seek(0, 0)
	require.Nil(t, err)

	t.ResetTimer()
	t.StartTimer()
	start := time.Now()
	for _ = range t.N {
		hasher := NewHasher(false)
		defer hasher.Close()
		io.CopyBuffer(hasher, file, make([]byte, 1024*1024))
		result, err := hasher.CookAndClose()
		require.Nil(t, err)
		require.Equal(t, result.Tlsh, "T172263a27b6050ef5f97c8279247a0322fa363d93aa0393136499f1cede766c05b9075b")
		require.Equal(t, result.Ssdeep, "98304:Oj0smILAkoTF+yiP+sPCvwgIblE50K627qr:OnVLAkojiP+sPCvwgIblE5xqr")
	}
	t.StopTimer()
	duration := time.Since(start)
	t.StopTimer()
	t.ReportMetric(float64(duration.Seconds()/float64(t.N)), "hash_time(s)")
}

func BenchmarkHashSpeedSmall(t *testing.B) {
	activeFileManager, err := testutils.NewFileManager()
	require.Nil(t, err)
	rawBytes, err := activeFileManager.DownloadFileBytes("1f0eeaa50f456887c306868ca7edf09cfecfd0d0529243a13c9cff22e32d7d15")
	if err != nil {
		return
	}
	file, err := os.CreateTemp("", "test-hasher")
	require.Nil(t, err)
	defer file.Close()
	defer os.Remove(file.Name())

	// Write whole file to disk.
	file.Write(rawBytes)
	_, err = file.Seek(0, 0)
	require.Nil(t, err)

	t.ResetTimer()
	t.StartTimer()
	start := time.Now()
	for _ = range t.N {
		hasher := NewHasher(false)
		defer hasher.Close()
		io.CopyBuffer(hasher, file, make([]byte, 1024*1024))
		result, err := hasher.CookAndClose()
		require.Nil(t, err)
		require.Equal(t, result.Tlsh, "T1d149330e7049d1ade2a34e7941f3cd16593337b2fc569d07dae0751c8a812da2e2b7a3")
		require.Equal(t, result.Ssdeep, "12582912:zf2dWiqviIECmhPdzcKDuyPldz1+VRjfoiylgKFuvWGA1b9s0+Ah:zkWdvildfDueGhylgLWVbd")
	}
	t.StopTimer()
	duration := time.Since(start)
	t.StopTimer()
	t.ReportMetric(float64(duration.Seconds()/float64(t.N)), "hash_time(s)")
}
