// Test the Flatten filesystem interface
package flatten_test

import (
	"os"
	"path/filepath"
	"testing"

	_ "github.com/rclone/rclone/backend/all" // for integration tests
	"github.com/rclone/rclone/backend/flatten"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/fstest/fstests"
)

// TestIntegration runs integration tests against a concrete remote
// set by the -remote flag. If the flag is not set, it creates a
// dynamic flatten overlay wrapping a local temporary directory.
func TestIntegration(t *testing.T) {
	opt := fstests.Opt{
		RemoteName:                   *fstest.RemoteName,
		NilObject:                    (*flatten.Object)(nil),
		SkipBadWindowsCharacters:     true,
		UnimplementableObjectMethods: []string{"Metadata", "SetMetadata", "MimeType", "ID", "SetTier", "GetTier"},
		UnimplementableFsMethods: []string{
			"ChangeNotify",
			"CleanUp",
			"Command",
			"DirCacheFlush",
			"DirMove",
			"DirSetModTime",
			"Disconnect",
			"MkdirMetadata",
			"MergeDirs",
			"OpenChunkWriter",
			"OpenWriterAt",
			"PublicLink",
			"PutStream",
			"PutUnchecked",
			"Shutdown",
			"UserInfo",
		},
	}
	if *fstest.RemoteName == "" {
		name := "TestFlatten"
		opt.RemoteName = name + ":"
		tempDir := filepath.Join(os.TempDir(), "rclone-flatten-test")
		opt.ExtraConfig = []fstests.ExtraConfigItem{
			{Name: name, Key: "type", Value: "flatten"},
			{Name: name, Key: "remote", Value: tempDir},
		}
		opt.QuickTestOK = true
	}
	fstests.Run(t, &opt)
}
