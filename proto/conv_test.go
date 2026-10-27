package proto

import (
	"errors"
	"io"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/stretchr/testify/require"
)

func sampleFileInfo() integration.FileInfo {
	return integration.FileInfo{
		Lname:      "hello.txt",
		Lsize:      1234,
		Lmode:      0o644,
		LmodTime:   time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Ldev:       1,
		Lino:       2,
		Luid:       1000,
		Lgid:       1000,
		Lnlink:     1,
		Lusername:  "alice",
		Lgroupname: "users",
		Flags:      0x42,
	}
}

func TestRecordRoundTrip_Regular(t *testing.T) {
	rec := &integration.Record{
		Pathname:           "/etc/hello.txt",
		FileInfo:           sampleFileInfo(),
		Target:             "",
		ExtendedAttributes: []string{"user.foo", "user.bar"},
		FileAttributes:     0xdead,
		Reader:             io.NopCloser(strings.NewReader("xx")),
	}

	pb := RecordToProto(rec)
	require.True(t, pb.HasReader, "regular file with reader should have HasReader=true")

	got, err := RecordFromProto(pb)
	require.NoError(t, err)
	require.Equal(t, rec.Pathname, got.Pathname)
	require.True(t, got.FileInfo.Equal(&rec.FileInfo), "fileinfo mismatch: %+v vs %+v", got.FileInfo, rec.FileInfo)
	require.Equal(t, rec.FileAttributes, got.FileAttributes)
}

func TestRecordRoundTrip_ErrorSkipsFileInfo(t *testing.T) {
	rec := &integration.Record{
		Pathname: "/missing",
		Err:      errors.New("ENOENT"),
	}

	pb := RecordToProto(rec)
	require.Nil(t, pb.FileInfo, "error record should not have FileInfo serialised")
	require.False(t, pb.HasReader, "error record should not advertise HasReader")
	require.Equal(t, "ENOENT", pb.Error)

	got, err := RecordFromProto(pb)
	require.NoError(t, err)
	require.EqualError(t, got.Err, "ENOENT")
}

func TestRecordRoundTrip_Xattr(t *testing.T) {
	rec := &integration.Record{
		Pathname:  "/etc/hello.txt",
		IsXattr:   true,
		XattrName: "user.comment",
		XattrType: integration.AttributeExtended,
		Reader:    io.NopCloser(strings.NewReader("data")),
	}

	pb := RecordToProto(rec)
	require.Nil(t, pb.FileInfo, "xattr record should not include FileInfo")
	require.True(t, pb.HasReader, "xattr record with a Reader should have HasReader=true")

	got, err := RecordFromProto(pb)
	require.NoError(t, err)
	require.True(t, got.IsXattr)
	require.Equal(t, "user.comment", got.XattrName)
}

func TestRecordFromProto_MissingFileInfoIsError(t *testing.T) {
	// Regular (non-xattr, no error) record arriving with no FileInfo
	// must fail rather than silently produce a zero FileInfo.
	bad := &Record{
		Pathname: "/somewhere",
	}
	_, err := RecordFromProto(bad)
	require.ErrorIs(t, err, ErrInvalidRecord)
}

func TestRecordToProto_NoReader_DirectoryHasNoReader(t *testing.T) {
	rec := &integration.Record{
		Pathname: "/etc",
		FileInfo: integration.FileInfo{
			Lname: "etc",
			Lmode: fs.ModeDir | 0o755,
		},
	}
	pb := RecordToProto(rec)
	require.False(t, pb.HasReader, "directory record should not advertise a reader")
}

func TestRecordToProto_RegularButNilReader(t *testing.T) {
	rec := &integration.Record{
		Pathname: "/etc/hello.txt",
		FileInfo: sampleFileInfo(),
	}
	pb := RecordToProto(rec)
	require.False(t, pb.HasReader, "nil reader should yield HasReader=false")
}

func TestResultRoundTrip(t *testing.T) {
	r := &integration.Result{
		Record: integration.Record{
			Pathname: "/etc/hello.txt",
			FileInfo: sampleFileInfo(),
		},
		Err: errors.New("boom"),
	}
	pb := ResultToProto(r)
	require.Equal(t, "boom", pb.Error)
	got, err := ResultFromProto(pb)
	require.NoError(t, err)
	require.EqualError(t, got.Err, "boom")
	require.Equal(t, r.Record.Pathname, got.Record.Pathname)
}
