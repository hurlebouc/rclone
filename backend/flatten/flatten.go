// Package flatten provides a virtual backend that flattens the file
// hierarchy. Files keep their full path names (dir/sub/file.txt) when
// accessed through this backend, but are stored in the wrapped remote
// under fixed-size flat names at its root. An index file stored in the
// wrapped remote maps the virtual path names to the flat names.
package flatten

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/cache"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fspath"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
)

// IndexName is the name of the index file stored in the wrapped remote.
const IndexName = ".rclone-flatten-index.json"

// Limits for the name_length option.
const (
	minNameLength = 8
	maxNameLength = 40
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "flatten",
		Description: "Flatten the file hierarchy into fixed-size file names",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:     "remote",
			Required: true,
			Help: `Remote to flatten.
Normally should contain a ':' and a path, e.g. "myremote:path/to/dir",
"myremote:bucket" or maybe "myremote:" (not recommended).`,
		}, {
			Name:     "name_length",
			Advanced: true,
			Default:  16,
			Help: fmt.Sprintf(`Length of the flat file names generated in the wrapped remote.
Must be between %d and %d. Flat names are derived from a hash of the
virtual path so they always have the same size. Increase this if you
have a very large number of files.`, minNameLength, maxNameLength),
		}},
	})
}

// Options defines the configuration for this backend.
type Options struct {
	Remote     string `config:"remote"`
	NameLength int    `config:"name_length"`
}

// Fs represents a wrapped fs.Fs.
type Fs struct {
	name     string
	root     string // virtual sub-root within the index name space
	base     fs.Fs  // wrapped remote holding the flat names and the index
	opt       Options
	features *fs.Features
	wrapper  fs.Fs

	mu    sync.RWMutex
	index map[string]string // virtual path -> flat name
	dirty bool
}

// NewFs constructs an Fs from the path, container:path
func NewFs(ctx context.Context, name, rpath string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}
	if opt.NameLength < minNameLength || opt.NameLength > maxNameLength {
		return nil, fmt.Errorf("name_length must be between %d and %d", minNameLength, maxNameLength)
	}
	if strings.HasPrefix(opt.Remote, name+":") {
		return nil, errors.New("can't point remote at itself - check the value of the remote setting")
	}
	baseFs, err := cache.Get(ctx, opt.Remote)
	if err != fs.ErrorIsFile && err != nil {
		return nil, fmt.Errorf("failed to make remote %q to wrap: %w", opt.Remote, err)
	}
	f := &Fs{
		base: baseFs,
		name: name,
		root: rpath,
		opt:  *opt,
	}
	cache.PinUntilFinalized(f.base, f)
	if err := f.loadIndex(ctx); err != nil {
		return nil, err
	}
	// Correct root if definitely pointing to a file
	if err == fs.ErrorIsFile || f.isIndexed(ctx, rpath) {
		f.root = path.Dir(f.root)
		if f.root == "." || f.root == "/" {
			f.root = ""
		}
		if err != fs.ErrorIsFile {
			err = fs.ErrorIsFile
		}
	}
	f.features = (&fs.Features{
		DuplicateFiles:          true,
		CanHaveEmptyDirectories: false,
	}).Fill(ctx, f).Mask(ctx, baseFs).WrapsFs(f, baseFs)
	f.features.ListR = f.ListR
	return f, err
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string { return f.name }

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string { return f.root }

// String returns a description of the FS
func (f *Fs) String() string {
	return fmt.Sprintf("flatten: '%s'", f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features { return f.features }

// Precision of the ModTimes in this Fs
func (f *Fs) Precision() time.Duration { return f.base.Precision() }

// Hashes returns the supported hash types of the filesystem
func (f *Fs) Hashes() hash.Set { return f.base.Hashes() }

// UnWrap returns the Fs that this Fs is wrapping
func (f *Fs) UnWrap() fs.Fs { return f.base }

// WrapFs returns the Fs that is wrapping this Fs
func (f *Fs) WrapFs() fs.Fs { return f.wrapper }

// SetWrapper sets the Fs that is wrapping this Fs
func (f *Fs) SetWrapper(wrapper fs.Fs) { f.wrapper = wrapper }

// fullRemote returns the index path for a path relative to the root.
func (f *Fs) fullRemote(remote string) string {
	return fspath.JoinRootPath(f.root, remote)
}

// loadIndex reads the index file from the wrapped remote, if present.
func (f *Fs) loadIndex(ctx context.Context) error {
	indexObj, err := f.base.NewObject(ctx, IndexName)
	if errors.Is(err, fs.ErrorObjectNotFound) || errors.Is(err, fs.ErrorDirNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	rc, err := indexObj.Open(ctx)
	if err != nil {
		return fmt.Errorf("failed to open flatten index: %w", err)
	}
	defer func() {
		_ = rc.Close()
	}()
	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("failed to read flatten index: %w", err)
	}
	var index map[string]string
	if err := json.Unmarshal(data, &index); err != nil {
		return fmt.Errorf("failed to parse flatten index: %w", err)
	}
	f.mu.Lock()
	f.index = index
	f.mu.Unlock()
	return nil
}

// saveIndex writes the index file to the wrapped remote if it has changed.
func (f *Fs) saveIndex(ctx context.Context) error {
	f.mu.Lock()
	if !f.dirty {
		f.mu.Unlock()
		return nil
	}
	data, err := json.Marshal(f.index)
	f.mu.Unlock()
	if err != nil {
		return err
	}
	src := object.NewStaticObjectInfo(IndexName, time.Now(), int64(len(data)), true, nil, f.base)
	if _, err := f.base.Put(ctx, strings.NewReader(string(data)), src); err != nil {
		return fmt.Errorf("failed to write flatten index: %w", err)
	}
	f.mu.Lock()
	f.dirty = false
	f.mu.Unlock()
	return nil
}

// isIndexed returns true if remote is a file mapped in the index.
func (f *Fs) isIndexed(ctx context.Context, remote string) bool {
	if remote == "" {
		return false
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, ok := f.index[remote]
	return ok
}

// flatNameFor returns the flat name of remote, generating a new one if
// needed. The caller must hold f.mu.
func (f *Fs) flatNameFor(remote string) string {
	if flat, ok := f.index[remote]; ok {
		return flat
	}
	used := make(map[string]bool, len(f.index))
	for _, flat := range f.index {
		used[flat] = true
	}
	flat := f.hashName(remote, 0)
	for n := 1; used[flat]; n++ {
		flat = f.hashName(remote, n)
	}
	f.index[remote] = flat
	f.dirty = true
	return flat
}

// hashName derives a flat name of the configured length from a virtual
// path, disambiguated by the round number on hash collisions.
func (f *Fs) hashName(remote string, round int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s#%d", remote, round)))
	encoded := base32.StdEncoding.EncodeToString(sum[:])
	return strings.ToLower(encoded[:f.opt.NameLength])
}

// listVirtual returns all virtual objects under the root and the set of
// virtual directory names, relative to the root, that contain them.
func (f *Fs) listVirtual(ctx context.Context) (objs map[string]fs.Object, dirs map[string]bool, err error) {
	f.mu.RLock()
	index := make(map[string]string, len(f.index))
	for k, v := range f.index {
		index[k] = v
	}
	f.mu.RUnlock()
	entries, err := f.base.List(ctx, "")
	if err != nil {
		return nil, nil, err
	}
	reverse := make(map[string]string, len(index))
	for remote, flat := range index {
		reverse[flat] = remote
	}
	objs = map[string]fs.Object{}
	dirs = map[string]bool{}
	rootPrefix := ""
	if f.root != "" {
		rootPrefix = f.root + "/"
	}
	for _, entry := range entries {
		obj, ok := entry.(fs.Object)
		if !ok || obj.Remote() == IndexName {
			continue
		}
		remote, ok := reverse[obj.Remote()]
		if !ok {
			// not referenced by the index, ignore
			continue
		}
		if !strings.HasPrefix(remote, rootPrefix) {
			continue
		}
		rel := remote[len(rootPrefix):]
		objs[rel] = f.newObject(obj, rel)
		for dir := path.Dir(rel); dir != "." && dir != "/" && dir != ""; dir = path.Dir(dir) {
			dirs[dir] = true
		}
	}
	return objs, dirs, nil
}

// List the objects and directories in dir into entries.
//
// dir should be "" to list the root, and should not have trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	objs, dirs, err := f.listVirtual(ctx)
	if err != nil {
		return nil, err
	}
	if dir != "" && !dirs[dir] {
		return nil, fs.ErrorDirNotFound
	}
	prefix := ""
	if dir != "" {
		prefix = dir + "/"
	}
	childDirs := map[string]bool{}
	for remote := range objs {
		if !strings.HasPrefix(remote, prefix) {
			continue
		}
		rest := remote[len(prefix):]
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			childDirs[prefix+rest[:i]] = true
		} else {
			entries = append(entries, objs[remote])
		}
	}
	for name := range childDirs {
		entries = append(entries, fs.NewDir(name, time.Time{}))
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].String() < entries[j].String()
	})
	return entries, nil
}

// ListR lists the objects and directories of the Fs recursively.
func (f *Fs) ListR(ctx context.Context, dir string, callback fs.ListRCallback) (err error) {
	objs, dirs, err := f.listVirtual(ctx)
	if err != nil {
		return err
	}
	prefix := ""
	if dir != "" {
		prefix = dir + "/"
	}
	var entries fs.DirEntries
	for name := range dirs {
		if strings.HasPrefix(name, prefix) {
			entries = append(entries, fs.NewDir(name, time.Time{}))
		}
	}
	for remote, obj := range objs {
		if strings.HasPrefix(remote, prefix) {
			entries = append(entries, obj)
		}
	}
	return callback(entries)
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error ErrorObjectNotFound.
//
// If remote points to a directory then it should return
// ErrorIsDir if possible without doing any extra work,
// otherwise ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	full := f.fullRemote(remote)
	f.mu.RLock()
	flat, ok := f.index[full]
	if !ok {
		// remote may be a directory prefix of a mapped file
		for mapped := range f.index {
			if strings.HasPrefix(mapped, full+"/") {
				f.mu.RUnlock()
				return nil, fs.ErrorIsDir
			}
		}
	}
	f.mu.RUnlock()
	if !ok {
		return nil, fs.ErrorObjectNotFound
	}
	o, err := f.base.NewObject(ctx, flat)
	if err != nil {
		return nil, err
	}
	return f.newObject(o, remote), nil
}

// newObject wraps a base object with a virtual remote name.
func (f *Fs) newObject(o fs.Object, remote string) fs.Object {
	return &Object{
		Object: o,
		f:      f,
		remote: remote,
	}
}

// putInto uploads a stream to the virtual path remote.
func (f *Fs) putInto(ctx context.Context, remote string, in io.Reader, src fs.ObjectInfo, options []fs.OpenOption) (fs.Object, error) {
	full := f.fullRemote(remote)
	f.mu.Lock()
	flat := f.flatNameFor(full)
	f.mu.Unlock()
	wrapped := object.NewStaticObjectInfo(flat, src.ModTime(ctx), src.Size(), true, nil, f.base)
	obj, err := f.base.Put(ctx, in, wrapped, options...)
	if err != nil {
		return nil, err
	}
	if err := f.saveIndex(ctx); err != nil {
		return nil, err
	}
	return f.newObject(obj, remote), nil
}

// Put in to the remote path with the modTime given of the given size
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Put should either
// return an error or upload it properly (rather than e.g. calling panic).
//
// May create the object even if it returns an error - if so
// will return the object and the error, otherwise will return
// nil and the error
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.putInto(ctx, src.Remote(), in, src, options)
}

// Mkdir makes the directory (container, bucket)
//
// Shouldn't return an error if it already exists
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	// Directories other than the wrapped root are virtual and need no storage
	if f.fullRemote(dir) != "" {
		return nil
	}
	return f.base.Mkdir(ctx, "")
}

// Rmdir removes the directory (container, bucket) if empty
//
// Return an error if it doesn't exist or isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	entries, err := f.List(ctx, dir)
	if err != nil && !errors.Is(err, fs.ErrorDirNotFound) {
		return err
	}
	if len(entries) > 0 {
		return fs.ErrorDirectoryNotEmpty
	}
	if f.fullRemote(dir) != "" {
		// Virtual directories leave no trace in the wrapped remote
		return nil
	}
	// Removing the wrapped root also removes the now useless index file
	if indexObj, err := f.base.NewObject(ctx, IndexName); err == nil {
		if err := indexObj.Remove(ctx); err != nil {
			return err
		}
		f.mu.Lock()
		f.index = map[string]string{}
		f.dirty = false
		f.mu.Unlock()
	}
	return f.base.Rmdir(ctx, "")
}

// Purge removes the directory and all of its contents.
func (f *Fs) Purge(ctx context.Context, dir string) error {
	objs, _, err := f.listVirtual(ctx)
	if err != nil {
		return err
	}
	prefix := ""
	if dir != "" {
		prefix = dir + "/"
	}
	found := false
	for remote, obj := range objs {
		if !strings.HasPrefix(remote, prefix) {
			continue
		}
		found = true
		if err := obj.Remove(ctx); err != nil {
			return err
		}
	}
	if f.fullRemote(dir) != "" {
		if !found {
			return fs.ErrorDirNotFound
		}
		return f.saveIndex(ctx)
	}
	// Purging the wrapped root also removes the index and the root itself
	if indexObj, err := f.base.NewObject(ctx, IndexName); err == nil {
		if err := indexObj.Remove(ctx); err != nil {
			return err
		}
	}
	f.mu.Lock()
	f.index = map[string]string{}
	f.dirty = false
	f.mu.Unlock()
	return f.base.Rmdir(ctx, "")
}

// Copy copies src to this remote into remote.
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	if srcRemote := src.Remote(); srcRemote == remote {
		return f.NewObject(ctx, remote)
	}
	in, err := src.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = in.Close()
	}()
	return f.putInto(ctx, remote, in, src, nil)
}

// Move moves src to this remote into remote.
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	so, ok := src.(*Object)
	if !ok {
		obj, err := f.Copy(ctx, src, remote)
		if err != nil {
			return nil, err
		}
		if err := src.Remove(ctx); err != nil {
			return nil, err
		}
		return obj, nil
	}
	if so.remote == remote {
		return so, nil
	}
	full := f.fullRemote(remote)
	oldFull := f.fullRemote(so.remote)
	f.mu.Lock()
	flat := f.index[oldFull]
	if old, ok := f.index[full]; ok && old != flat {
		f.dirty = true
		f.mu.Unlock()
		if oldObj, err := f.base.NewObject(ctx, old); err == nil {
			if err := oldObj.Remove(ctx); err != nil {
				return nil, err
			}
		}
		f.mu.Lock()
	}
	delete(f.index, oldFull)
	f.index[full] = flat
	f.dirty = true
	f.mu.Unlock()
	if err := f.saveIndex(ctx); err != nil {
		return nil, err
	}
	o, err := f.base.NewObject(ctx, flat)
	if err != nil {
		return nil, err
	}
	return f.newObject(o, remote), nil
}

// About gets quota information from the Fs.
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	do, ok := f.base.(fs.Abouter)
	if !ok {
		return nil, fs.ErrorNotImplemented
	}
	return do.About(ctx)
}

// DirCacheFlush resets the directory cache.
func (f *Fs) DirCacheFlush() {
	if do, ok := f.base.(fs.DirCacheFlusher); ok {
		do.DirCacheFlush()
	}
}

// Object describes a wrapped file in the flatten Fs.
type Object struct {
	fs.Object
	f      *Fs
	remote string
}

// Fs returns the Fs that this Object is part of
func (o *Object) Fs() fs.Info { return o.f }

// Remote returns the remote name of the object
func (o *Object) Remote() string { return o.remote }

// String returns a description of the Object
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Update in to the object with the modTime given of the given size.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	remote := src.Remote()
	flat := o.Object.Remote()
	full := o.f.fullRemote(remote)
	o.f.mu.Lock()
	if old, ok := o.f.index[full]; !ok || old != flat {
		o.f.index[full] = flat
		o.f.dirty = true
	}
	o.f.mu.Unlock()
	wrapped := object.NewStaticObjectInfo(flat, src.ModTime(ctx), src.Size(), true, nil, o.f.base)
	if err := o.Object.Update(ctx, in, wrapped, options...); err != nil {
		return err
	}
	o.remote = remote
	return o.f.saveIndex(ctx)
}

// Remove removes the object from the wrapped remote and from the index.
func (o *Object) Remove(ctx context.Context) error {
	if err := o.Object.Remove(ctx); err != nil {
		return err
	}
	o.f.mu.Lock()
	delete(o.f.index, o.f.fullRemote(o.remote))
	o.f.dirty = true
	o.f.mu.Unlock()
	return o.f.saveIndex(ctx)
}

// UnWrap returns the wrapped Object
func (o *Object) UnWrap() fs.Object { return o.Object }

// Check interfaces
var (
	_ fs.Fs              = (*Fs)(nil)
	_ fs.Purger          = (*Fs)(nil)
	_ fs.Copier          = (*Fs)(nil)
	_ fs.Mover           = (*Fs)(nil)
	_ fs.ListRer         = (*Fs)(nil)
	_ fs.Abouter         = (*Fs)(nil)
	_ fs.DirCacheFlusher = (*Fs)(nil)
	_ fs.UnWrapper       = (*Fs)(nil)
	_ fs.Wrapper         = (*Fs)(nil)
	_ fs.Object          = (*Object)(nil)
)
