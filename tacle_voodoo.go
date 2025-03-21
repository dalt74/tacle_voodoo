/*
This software is not permitted to use in:

* United Kingdom
* Australia and New Zealand
* United States
* Canada
* Estonia
* Lithuania
* Latvia
* Ukraine
* France
* Germany
* Spain
* Italy
* Finland

Also not permitted to use or distrubetd by or for the
interest of any resident citizen or resident company of
the any country specified.

Otherwise you are free to use and distribute the
software as specified AGPLv3 untill you keep
this policy note
*/
package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
)

var not_impl = fmt.Errorf("not_implemented")
var unmatched = fmt.Errorf("unmatched")
var not_found = fmt.Errorf("unfound")

const magic_version_ofset = 83
const MB = 1024 * 1024

type ObjectAddress struct {
	osdnum   int
	pool     int64
	pg       int
	prefix   string
	name     string
	size     int64
	snap_ref int64
	version  int64
}

type Volume struct {
	id        int64
	Parent    *Snapshot
	pool      int64
	name      string
	order     int
	size      int64
	Snapshots map[string]*Snapshot
}

type Snapshot struct {
	snap_ref int64
	name     string
	size     int64
	Volume   *Volume
}

type AtomicOperation struct {
	objects       []*ObjectAddress
	size          int64
	object_offset int64
}

func (op *AtomicOperation) ToString() string {
	if len(op.objects) > 0 {
		snap_ref := max(op.objects[0].snap_ref, 0)
		return fmt.Sprintf(
			"%s#%d range [%d - %d]",
			op.objects[0].name, snap_ref, op.object_offset, op.object_offset+op.size-1,
		)
	} else {
		return fmt.Sprintf("zero %d bytes", op.size)
	}
}

func NewAtomicOperation(
	address []*ObjectAddress,
	object_offset int64,
	operation_size int64,
) *AtomicOperation {
	return &AtomicOperation{
		objects:       append(make([]*ObjectAddress, 0, len(address)), address...),
		object_offset: object_offset,
		size:          operation_size,
	}
}

type DataSource interface {
	BlockSize() int64
	GetSize() int64
	GetParent() *Snapshot
	GetBaseName() string
	GetSnapRef() int64
	GetPool() int64
	GetName() string
}

func (v *Volume) GetName() string {
	return v.name
}

func (v *Volume) GetPool() int64 {
	return v.pool
}

func (v *Volume) GetBaseName() string {
	return fmt.Sprintf("rbd_data.%x", v.id)
}

func (v *Volume) GetSnapRef() int64 {
	return -1
}

func (v *Volume) BlockSize() int64 {
	return 2 << (v.order - 1)
}

func (v *Volume) GetParent() *Snapshot {
	return v.Parent
}

func (v *Volume) GetSize() int64 {
	return v.size
}

func (s *Snapshot) GetBaseName() string {
	if s.Volume == nil {
		fmt.Println("Snap", s.name, "has no volume(?)")
	}
	return s.Volume.GetBaseName()
}

func (v *Snapshot) GetPool() int64 {
	return v.Volume.pool
}

func (s *Snapshot) GetName() string {
	return fmt.Sprintf("%s@%s", s.Volume.name, s.name)
}

func (s *Snapshot) GetSnapRef() int64 {
	return s.snap_ref
}

func (v *Snapshot) BlockSize() int64 {
	return 2 << (v.Volume.order - 1)
}

func (s *Snapshot) GetParent() *Snapshot {
	if s.Volume == nil {
		fmt.Println("Snapshot", s.name, "has no volume (?)")
	}
	return s.Volume.Parent
}

func (s *Snapshot) GetSize() int64 {
	return s.size
}

func GetGranularity(src DataSource) int64 {
	g := src.BlockSize()
	parent := src.GetParent()
	if parent != nil {
		pg := GetGranularity(parent)
		if pg < g {
			g = pg
		}
	}
	return g
}

var is_debug = false

func Debugf(f string, args ...any) {
	if is_debug {
		fmt.Print("DEBUG: ")
		fmt.Printf(f, args...)
		fmt.Println()
	}
}

func GetBlockOps(src DataSource, offset int64, size int64, ctx *FullContext) ([]*AtomicOperation, error) {
	ret := make([]*AtomicOperation, 0)
	blocksize := src.BlockSize()
	obj_name := fmt.Sprintf("%s.%016x", src.GetBaseName(), offset/blocksize)
	snap_ref := src.GetSnapRef()
	if offset > src.GetSize() {
		ret = append(ret, NewAtomicOperation(nil, 0, size))
		return ret, nil
	}

	objlist, err := FindObject(ctx.Store, src.GetPool(), obj_name, snap_ref)
	if err != nil {
		return nil, err
	}
	if len(objlist) == 0 {
		if src.GetParent() != nil {
			if ops, err := GetBlockOps(src.GetParent(), offset, size, ctx); err != nil {
				return nil, err
			} else {
				ret = append(ret, ops...)
				return ret, nil
			}
		} else {
			ret = append(ret, NewAtomicOperation(nil, 0, size))
			return ret, nil
		}
	}
	object_size := objlist[0].size
	delta := offset % object_size
	usable := object_size - delta
	to_use := min(usable, size)
	if to_use > 0 {
		ret = append(ret, NewAtomicOperation(objlist, delta, to_use))
		size -= to_use
		offset += to_use
	}
	if size > 0 {
		if src.GetParent() != nil {
			if ops, err := GetBlockOps(src.GetParent(), offset, size, ctx); err != nil {
				return nil, err
			} else {
				ret = append(ret, ops...)
			}
		} else {
			ret = append(ret, NewAtomicOperation(objlist, 0, size))
		}
	}
	return ret, nil
}

func GetPlan2(src DataSource, ctx *FullContext) ([]*AtomicOperation, error) {
	ret := make([]*AtomicOperation, 0)
	offset := int64(0)
	size := src.GetSize()
	gran := GetGranularity(src)
	for offset < size {
		opsize := gran
		if offset+opsize > size {
			opsize = size - offset
		}
		if ops, err := GetBlockOps(src, offset, opsize, ctx); err != nil {
			return nil, err
		} else {
			ret = append(ret, ops...)
		}
		offset += opsize
	}
	sum := int64(0)
	for _, op := range ret {
		Debugf("Entry: %s", op.ToString())
		sum += op.size
	}
	Debugf("Total size: %dMB", sum/MB)
	return ret, nil
}

func (v *Volume) GetSnapByRef(snapref int64) (*Snapshot, error) {
	for _, snap := range v.Snapshots {
		if snap.snap_ref == snapref {
			return snap, nil
		}
	}
	return nil, not_found
}

func StartsWith(s string, sub string) bool {
	return strings.Index(s, sub) == 0
}

func ReadVolume(path string) (*Volume, error) {
	if _, err := os.ReadFile(path); err != nil {
		return nil, err
	} else {
		return nil, not_impl
	}
}

func UnpackPg(path string) (int64, int) {
	tokens := strings.Split(path, "_")
	tokens = strings.Split(tokens[0], ".")
	pool, _ := strconv.ParseInt(tokens[0], 16, 64)
	pg, _ := strconv.ParseInt(tokens[1], 16, 32)
	return pool, int(pg)
}

func ScanOsd(osdpath string, osdnum int) ([]*ObjectAddress, error) {
	ret := make([]*ObjectAddress, 0, 10000)
	if entries, err := os.ReadDir(osdpath); err != nil {
		return nil, err
	} else {
		for _, item := range entries {
			if !strings.Contains(item.Name(), "_head") {
				continue
			}
			pool, pg := UnpackPg(item.Name())
			if objects, err := ScanPg(osdpath, osdnum, pool, pg); err != nil {
				return nil, err
			} else {
				ret = append(ret, objects...)
			}
		}
		return ret, nil
	}
}

func ScanPg(osdpath string, osdnum int, pool int64, pg int) ([]*ObjectAddress, error) {
	ret := make([]*ObjectAddress, 0, 10000)
	opath := fmt.Sprintf("%s/%d.%x_head/all", osdpath, pool, pg)
	if entries, err := os.ReadDir(opath); err != nil {
		return nil, err
	} else {
		for _, item := range entries {
			if obj, err := ParseObjectByName(pool, pg, item.Name()); err == nil {
				obj.osdnum = osdnum
				ret = append(ret, obj)
			} else {
				fmt.Printf("Failed parse %s/%s - %s\n", opath, item.Name(), err)
			}
		}
		return ret, nil
	}
}

func ParseObjectByName(pool int64, pg int, basename string) (*ObjectAddress, error) {
	tokens := strings.Split(basename, ":")
	if len(tokens) != 6 {
		return nil, unmatched
	}
	if tokens[4] == "" {
		return nil, unmatched
	}
	snap_ref := int64(0)
	var err error
	if tokens[5] == "head#" {
		snap_ref = -1
	} else if snap_ref, err = strconv.ParseInt(tokens[5][0:len(tokens[5])-1], 16, 64); err != nil {
		return nil, unmatched
	}
	ret := &ObjectAddress{
		pool: pool, pg: pg,
		prefix:   fmt.Sprintf("%s:%s:::", tokens[0], tokens[1]),
		name:     tokens[4],
		snap_ref: snap_ref,
	}
	return ret, nil
}

func ByteSliceToInt64(data []byte) int64 {
	ret := int64(0)
	scale := int64(1)
	for i := range 8 {
		ret += scale * int64(data[i])
		if i < 7 {
			scale = scale * 256
		}
	}
	return ret
}

func ByteSliceToInt(data []byte) int {
	ret := 0
	scale := 1
	for i := range 4 {
		ret += scale * int(data[i])
		if i < 3 {
			scale = scale * 256
		}
	}
	return ret
}

func ReadObjectMeta(osdpath string, obj *ObjectAddress) error {
	suffix := "head#"
	if obj.snap_ref > 0 {
		obj.version = 0
		suffix = fmt.Sprintf("%x#", obj.snap_ref)
	}
	xdir := fmt.Sprintf("%s/%d.%x_head/all/%s%s:%s", osdpath, obj.pool, obj.pg, obj.prefix, obj.name, suffix)
	path := fmt.Sprintf("%s/attr/_", xdir)
	if _data, err := os.ReadFile(path); err == nil {
		base := magic_version_ofset + len(obj.name)
		vdata := _data[base : base+8]
		obj.version = ByteSliceToInt64(vdata)
	}
	path = fmt.Sprintf("%s/data", xdir)
	if sd, err := os.Stat(path); err == nil {
		obj.size = sd.Size()
	} else {
		fmt.Printf("failed to stat %s/data\n", xdir)
	}
	return nil
}

type ObjectLister interface {
	ListObjects(pool int64, name_or_prefix string, suggest_prefix bool) ([]*ObjectAddress, error)
	Load() error
	Save() error
	Size() int
}

type ObjectStore interface {
	ObjectLister
	AddObject(*ObjectAddress) error
}

type MultiStore struct {
	listers []ObjectLister
}

func NewMultiStore() *MultiStore {
	return &MultiStore{
		listers: make([]ObjectLister, 0, 100),
	}
}

func (ms *MultiStore) Load() error {
	for _, l := range ms.listers {
		if err := l.Load(); err != nil {
			return err
		}
	}
	return nil
}

func (ms *MultiStore) Save() error {
	for _, l := range ms.listers {
		if err := l.Save(); err != nil {
			return err
		}
	}
	return nil
}

func (ms *MultiStore) Size() int {
	ret := 0
	for _, l := range ms.listers {
		ret += l.Size()
	}
	return ret
}

func (ms *MultiStore) ListObjects(pool int64, name_or_prefix string, suggest_prefix bool) ([]*ObjectAddress, error) {
	ret := make([]*ObjectAddress, 0, 100)
	for _, ls := range ms.listers {
		if s, err := ls.ListObjects(pool, name_or_prefix, suggest_prefix); err != nil {
			return nil, err
		} else {
			ret = append(ret, s...)
		}
	}
	return ret, nil
}

type GenericObjectStore struct{}

func (*GenericObjectStore) Size() int { return 0 }

func (*GenericObjectStore) AddObject(obj *ObjectAddress) error { return not_impl }

func (*GenericObjectStore) ListObjects(pool int64, name_or_prefix string, suggest_prefix bool) ([]*ObjectAddress, error) {
	return nil, not_impl
}

func FindObject(store ObjectLister, pool int64, name string, snapref int64) ([]*ObjectAddress, error) {
	if matched, err := store.ListObjects(pool, name, false); err != nil {
		return nil, err
	} else {
		all := make([]*ObjectAddress, 0, 10)
		for _, addr := range matched {
			if addr.pool != pool {
				continue
			} else if addr.name != name {
				continue
			} else {
				if snapref < 0 {
					if addr.snap_ref == -1 {
						all = append(all, addr)
					}
				} else {
					if addr.snap_ref >= snapref || addr.snap_ref == -1 {
						all = append(all, addr)
					}
				}
			}
		}
		slices.SortFunc(all, ObjectCompare)
		if len(all) > 1 {
			maxver := all[0].version
			isnapref := all[0].snap_ref
			for _, addr := range all {
				if addr.version > maxver && isnapref == addr.snap_ref {
					maxver = addr.version
				}
			}
			nall := make([]*ObjectAddress, 0, len(all))
			for _, addr := range all {
				if addr.version == maxver && addr.snap_ref == isnapref {
					nall = append(nall, addr)
				}
			}
			all = nall
		}
		return all, nil
	}
}

type InMemoryObjectStore struct {
	GenericObjectStore
	objects []*ObjectAddress
}

func (store *InMemoryObjectStore) Size() int { return len(store.objects) }

func (store *InMemoryObjectStore) AddObject(obj *ObjectAddress) error {
	store.objects = append(store.objects, obj)
	return nil
}

func (store *InMemoryObjectStore) ListObjects(pool int64, name_or_prefix string, suggest_prefix bool) ([]*ObjectAddress, error) {
	ret := make([]*ObjectAddress, 0, 100)
	slen := len(name_or_prefix)
	for _, addr := range store.objects {
		if !suggest_prefix {
			if name_or_prefix == addr.name {
				ret = append(ret, addr)
			}
		}
		if addr.pool != pool {
			continue
		} else if !suggest_prefix {
			if name_or_prefix == addr.name {
				ret = append(ret, addr)
			}
		} else if len(addr.name) < slen {
			continue
		} else if addr.name[0:slen] == name_or_prefix {
			ret = append(ret, addr)
		}
	}
	return ret, nil
}

func ObjectCompare(l *ObjectAddress, r *ObjectAddress) int {
	if l.snap_ref < 0 && r.snap_ref < 0 {
		if l.version > r.version {
			return -1
		} else if l.version == r.version {
			return 0
		} else {
			return -1
		}
	} else if l.snap_ref < 0 && r.snap_ref > 0 {
		return 1
	} else if l.snap_ref > 0 && r.snap_ref < 0 {
		return -1
	} else if l.snap_ref < r.snap_ref {
		return -1
	} else if l.snap_ref > r.snap_ref {
		return 1
	} else {
		return 0
	}
}

func (store *InMemoryObjectStore) Load() error {
	return nil
}

func (store *InMemoryObjectStore) Save() error {
	return not_impl
}

func InitInMemoryObjectStore(store *InMemoryObjectStore) *InMemoryObjectStore {
	store.objects = make([]*ObjectAddress, 0, 100000)
	return store
}

func NewInMemoryObjectStore() ObjectStore {
	return InitInMemoryObjectStore(&InMemoryObjectStore{})
}

type DebugObjectStore struct {
	InMemoryObjectStore
}

func (dos *DebugObjectStore) AddObject(obj *ObjectAddress) error {
	return dos.InMemoryObjectStore.AddObject(obj)
}

func NewDebugObjectStore() ObjectStore {
	ret := &DebugObjectStore{}
	InitInMemoryObjectStore(&ret.InMemoryObjectStore)
	return ret
}

type CSVObjectStore struct {
	InMemoryObjectStore
	filename string
}

func (dos *CSVObjectStore) AddObject(obj *ObjectAddress) error {
	return dos.InMemoryObjectStore.AddObject(obj)
}

func NewCSVObjectStore(path string) ObjectStore {
	ret := &CSVObjectStore{filename: path}
	InitInMemoryObjectStore(&ret.InMemoryObjectStore)
	return ret
}

func (store *CSVObjectStore) Load() error {
	if f, err := os.Open(store.filename); err != nil {
		return nil
	} else {
		defer f.Close()
		reader := csv.NewReader(f)
		for {
			tokens, err := reader.Read()
			if err == io.EOF {
				return nil
			} else if err != nil {
				return err
			}
			objaddr := &ObjectAddress{}
			if objaddr.osdnum, err = strconv.Atoi(tokens[0]); err != nil {
				return err
			}
			if objaddr.pool, err = strconv.ParseInt(tokens[1], 10, 64); err != nil {
				return err
			}
			if objaddr.pg, err = strconv.Atoi(tokens[2]); err != nil {
				return err
			}
			objaddr.prefix = tokens[3]
			objaddr.name = tokens[4]
			if objaddr.snap_ref, err = strconv.ParseInt(tokens[5], 10, 64); err != nil {
				return err
			}
			if objaddr.version, err = strconv.ParseInt(tokens[6], 10, 64); err != nil {
				return err
			}
			if objaddr.size, err = strconv.ParseInt(tokens[7], 10, 64); err != nil {
				return err
			}
			store.AddObject(objaddr)
		}
	}
}

func Itoa(n int) string   { return fmt.Sprintf("%d", n) }
func Ltoa(n int64) string { return fmt.Sprintf("%d", n) }

func (store *CSVObjectStore) Save() error {
	if f, err := os.OpenFile(store.filename, os.O_WRONLY|os.O_CREATE, 0644); err != nil {
		return err
	} else {
		defer f.Close()
		writer := csv.NewWriter(f)
		for _, e := range store.objects {
			vals := make([]string, 0, 8)
			vals = append(vals, Itoa(e.osdnum), Ltoa(e.pool), Itoa(e.pg))
			vals = append(vals, e.prefix, e.name)
			vals = append(vals, Ltoa(e.snap_ref), Ltoa(e.version), Ltoa(e.size))
			if err := writer.Write(vals); err != nil {
				return err
			}
		}
		writer.Flush()
		return nil
	}
}

type OSDAccessor interface {
	ListOmaps(obj *ObjectAddress) ([]string, error)
	GetOmap(obj *ObjectAddress, name string) ([]byte, error)
	GetData(obj *ObjectAddress) ([]byte, error)
}

type LocalOSDAccessor struct {
	basepath string
}

func (oa *LocalOSDAccessor) ListOmaps(obj *ObjectAddress) ([]string, error) {
	snap_ref := "head#"
	if obj.snap_ref > 0 {
		snap_ref = fmt.Sprintf("%x", obj.snap_ref)
	}
	fpath := fmt.Sprintf(
		"%s/%d.%x_head/all/%s%s:%s/omap",
		oa.basepath, obj.pool, obj.pg, obj.prefix, obj.name, snap_ref,
	)
	if ret, err := os.ReadDir(fpath); err != nil {
		return nil, not_found
	} else {
		aret := make([]string, 0, len(ret))
		for _, s := range ret {
			if s.Name() != "." && s.Name() != ".." {
				aret = append(aret, s.Name())
			}
		}
		return aret, nil
	}
}

func (oa *LocalOSDAccessor) GetOmap(obj *ObjectAddress, name string) ([]byte, error) {
	snap_ref := "head#"
	if obj.snap_ref > 0 {
		snap_ref = fmt.Sprintf("%x", obj.snap_ref)
	}
	path := fmt.Sprintf(
		"%s/%d.%x_head/all/%s%s:%s/omap/%s",
		oa.basepath, obj.pool, obj.pg, obj.prefix, obj.name, snap_ref, name,
	)
	if !FileExists(path) {
		return nil, not_found
	} else if fd, err := os.Open(path); err != nil {
		return nil, not_found
	} else {
		defer fd.Close()
		if data, err := io.ReadAll(fd); err != nil {
			return nil, not_found
		} else {
			return data, nil
		}
	}
}

func FileExists(fpath string) bool {
	if _, err := os.Stat(fpath); err != nil {
		return false
	} else {
		return true
	}
}

func (oa *LocalOSDAccessor) GetData(obj *ObjectAddress) ([]byte, error) {
	snap_ref := "head#"
	if obj.snap_ref > 0 {
		snap_ref = fmt.Sprintf("%x#", obj.snap_ref)
	}
	path := fmt.Sprintf(
		"%s/%d.%x_head/all/%s%s:%s/data",
		oa.basepath, obj.pool, obj.pg, obj.prefix, obj.name, snap_ref,
	)
	if !FileExists(path) {
		fmt.Println("ERROR: file not found", path)
		return nil, not_found
	} else if fd, err := os.Open(path); err != nil {
		return nil, err
	} else {
		defer fd.Close()
		if data, err := io.ReadAll(fd); err != nil {
			return nil, err
		} else {
			return data, nil
		}
	}
}

func NewLocalOSDAccessor(basepath string) OSDAccessor {
	return &LocalOSDAccessor{
		basepath: basepath,
	}
}

type HttpOSDAccessor struct {
	baseurl string
}

func (oa *HttpOSDAccessor) ListOmaps(obj *ObjectAddress) ([]string, error) {
	snap_ref := "head#"
	if obj.snap_ref > 0 {
		snap_ref = fmt.Sprintf("%x#", obj.snap_ref)
	}
	url := fmt.Sprintf(
		"%s/%d.%x_head/all/%s%s:%s/omap",
		oa.baseurl, obj.pool, obj.pg,
		url.PathEscape(obj.prefix), url.PathEscape(obj.name), url.PathEscape(snap_ref),
	)
	if resp, err := http.Get(url); err != nil {
		return nil, err
	} else if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Remote error: %d", resp.StatusCode)
	} else {
		ret := make([]byte, 0, 1024*1024)
		for {
			buf := make([]byte, 1024*1024)
			cnt, err := resp.Body.Read(buf)
			if cnt > 0 {
				ret = append(ret, buf[0:cnt]...)
			}
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}
		lret := make([]string, 0, 128)
		if err := json.Unmarshal(ret, &lret); err != nil {
			return nil, err
		}
		return lret, nil
	}
}

func (oa *HttpOSDAccessor) GetOmap(obj *ObjectAddress, name string) ([]byte, error) {
	snap_ref := "head#"
	if obj.snap_ref > 0 {
		snap_ref = fmt.Sprintf("%x#", obj.snap_ref)
	}
	url := fmt.Sprintf(
		"%s/%d.%x_head/all/%s%s:%s/omap/%s",
		oa.baseurl, obj.pool, obj.pg,
		url.PathEscape(obj.prefix), url.PathEscape(obj.name), url.PathEscape(snap_ref),
		name,
	)
	if resp, err := http.Get(url); err != nil {
		return nil, err
	} else if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Remote error: %d", resp.StatusCode)
	} else {
		ret := make([]byte, 0, 4*1024*1024)
		for {
			buf := make([]byte, 1024*1024)
			cnt, err := resp.Body.Read(buf)
			if cnt > 0 {
				ret = append(ret, buf[0:cnt]...)
			}
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}
		return ret, nil
	}
}

func (oa *HttpOSDAccessor) GetData(obj *ObjectAddress) ([]byte, error) {
	snap_ref := "head#"
	if obj.snap_ref > 0 {
		snap_ref = fmt.Sprintf("%x#", obj.snap_ref)
	}
	url := fmt.Sprintf(
		"%s/%d.%x_head/all/%s%s:%s/data",
		oa.baseurl, obj.pool, obj.pg,
		url.PathEscape(obj.prefix), url.PathEscape(obj.name), url.PathEscape(snap_ref),
	)
	if resp, err := http.Get(url); err != nil {
		return nil, err
	} else if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Remote error: %d", resp.StatusCode)
	} else {
		ret := make([]byte, 0, 4*1024*1024)
		for {
			buf := make([]byte, 1024*1024)
			cnt, err := resp.Body.Read(buf)
			if cnt > 0 {
				ret = append(ret, buf[0:cnt]...)
			}
			if err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}
		}
		return ret, nil
	}
}

func NewHttpOSDAccessor(basepath string) OSDAccessor {
	return &HttpOSDAccessor{
		baseurl: basepath,
	}
}

func run_scan(objectstore ObjectStore, xargs []string) {
	for _, arg := range xargs {
		tokens := strings.Split(arg, "=")
		if len(tokens) != 2 {
			Debugf("Omit arg %s", arg)
			continue
		} else if osdnum, err := strconv.Atoi(tokens[0]); err != nil {
			Debugf("Omit arg %s", arg)
			continue
		} else if items, err := ScanOsd(tokens[1], osdnum); err != nil {
			Debugf("Error scanning OSD: %s", err)
		} else {
			for _, obj := range items {
				if err := ReadObjectMeta(tokens[1], obj); err != nil {
					Debugf("Failed get meta for osd %d object %d.%x %s:%x - %s", obj.osdnum, obj.pool, obj.pg, obj.name, obj.snap_ref, err)
				}
			}
			for _, obj := range items {
				objectstore.AddObject(obj)
			}
		}
	}
	if err := objectstore.Save(); err != nil {
		fmt.Println("error saving objects:", err)
	} else {
		fmt.Printf("%d objects stored\n", objectstore.Size())
	}
}

func SimplifyPath(path string) (string, error) {
	ptokens := strings.Split(path, "/")
	ret := make([]string, 0, len(ptokens))
	for _, i := range ptokens {
		if i == "." {
			continue
		} else if i == ".." {
			if len(ret) > 1 {
				ret = ret[0 : len(ret)-1]
			} else if len(ret) > 0 {
				ret = make([]string, 0, len(ptokens))
			}
		} else if i == "" {
			continue
		} else {
			if s, err := url.PathUnescape(i); err != nil {
				return "", err
			} else {
				ret = append(ret, s)
			}
		}
	}
	return strings.Join(ret, "/"), nil
}

func run_server(wg *sync.WaitGroup, paddr string, proot string) {
	defer wg.Done()
	handler := func(w http.ResponseWriter, req *http.Request) {
		spath, err := SimplifyPath(req.URL.Path)
		if err != nil {
			w.Header().Add("Content-Type", "application/text")
			w.WriteHeader(400)
		} else {
			apath := fmt.Sprintf("%s/%s", proot, spath)
			if files, err := os.ReadDir(apath); err == nil {
				fmt.Println("Request list", spath, "from", apath, "OK")
				names := make([]string, 0, len(files))
				for _, s := range files {
					names = append(names, s.Name())
				}
				ret, _ := json.Marshal(names)
				w.Header().Add("Content-Type", "application/json")
				w.WriteHeader(200)
				w.Write(ret)
			} else if data, err := os.ReadFile(apath); err != nil {
				fmt.Println("Request data", spath, "from", apath, "FAILED:", err)
				if os.IsNotExist(err) {
					w.WriteHeader(404)
				} else if os.IsPermission(err) {
					w.WriteHeader(403)
				} else {
					w.WriteHeader(404)
				}
			} else {
				fmt.Println("Request data", spath, "from", apath, "OK")
				w.Header().Add("Content-Type", "application/octet-stream")
				w.WriteHeader(200)
				w.Write(data)
			}
		}
	}
	smux := http.NewServeMux()
	smux.HandleFunc("GET /", handler)
	fmt.Printf("Started server %s on %s\n", proot, paddr)
	http.ListenAndServe(paddr, smux)
}

func run_servers(roots []string) {
	wg := &sync.WaitGroup{}
	for _, root := range roots {
		if tokens := strings.Split(root, "="); len(tokens) != 2 {
			fmt.Printf("Invalid root: %s\n", root)
		} else {
			paddr := tokens[0]
			proot := tokens[1]
			if !strings.Contains(paddr, ":") {
				paddr = fmt.Sprintf("0.0.0.0:%s", paddr)
			}
			wg.Add(1)
			go run_server(wg, paddr, proot)
		}
	}
	wg.Wait()
}

func ReadPools() (map[string]int64, error) {
	conffile := os.Getenv("POOL_CONFIG")
	if conffile == "" {
		conffile = "pools.cfg"
	}
	if f, err := os.Open(conffile); err != nil {
		return nil, err
	} else {
		defer f.Close()
		sc := bufio.NewScanner(f)
		ret := make(map[string]int64)
		for sc.Scan() {
			l := sc.Text()
			l = strings.Split(l, "#")[0]
			l = strings.Trim(l, " \t\n\r")
			if len(l) == 0 {
				continue
			}
			t := strings.Split(l, " ")
			if len(t) != 2 {
				return nil, fmt.Errorf("bad pool ref %s, must be <Id> <Name>", l)
			}
			if n, err := strconv.ParseInt(t[0], 10, 64); err != nil {
				return nil, err
			} else {
				ret[t[1]] = n
			}
		}
		return ret, nil
	}
}

func ReadOSDs() (map[int]OSDAccessor, error) {
	conffile := os.Getenv("OSD_CONFIG")
	if conffile == "" {
		conffile = "osd.cfg"
	}
	if f, err := os.Open(conffile); err != nil {
		return nil, err
	} else {
		defer f.Close()
		sc := bufio.NewScanner(f)
		ret := make(map[int]OSDAccessor)
		for sc.Scan() {
			l := sc.Text()
			l = strings.Split(l, "#")[0]
			l = strings.Trim(l, " \t\n\r")
			if len(l) == 0 {
				continue
			}
			t := strings.Split(l, " ")
			if len(t) != 3 {
				return nil, fmt.Errorf("bad osd ref %s, must be <Id> <protocol> <Location>", l)
			}
			if n, err := strconv.Atoi(t[0]); err != nil {
				return nil, err
			} else if t[1] == "filesystem" {
				ret[n] = NewLocalOSDAccessor(t[2])
			} else if t[1] == "http" {
				ret[n] = NewHttpOSDAccessor(t[2])
			} else {
				return nil, fmt.Errorf("unsupported OSD protocol %s, use filesystem or url", t[1])
			}
		}
		return ret, nil
	}
}

func CreateObjectStore(spec string) (ObjectStore, error) {
	if spec == "debug" {
		return NewDebugObjectStore(), nil
	} else if strings.Index(spec, "csv:") == 0 {
		filename := spec[4:]
		return NewCSVObjectStore(filename), nil
	} else {
		return nil, fmt.Errorf("unsupported objectstore %s", spec)
	}
}

func LoadMultiStore(stores []string) (*MultiStore, error) {
	ms := NewMultiStore()
	for _, ref := range stores {
		if store, err := CreateObjectStore(ref); err != nil {
			return nil, err
		} else if err := store.Load(); err != nil {
			return nil, err
		} else {
			ms.listers = append(ms.listers, store)
		}
	}
	return ms, nil
}

type FullContext struct {
	Pools map[string]int64
	OSDs  map[int]OSDAccessor
	Store *MultiStore
}

func (fc *FullContext) ListOmaps(objs []*ObjectAddress) ([]string, error) {
	for _, obj := range objs {
		if osda, found := fc.OSDs[obj.osdnum]; !found {
			continue
		} else if ret, err := osda.ListOmaps(obj); err != nil {
			continue
		} else {
			return ret, nil
		}
	}
	return nil, fmt.Errorf("not available")
}

func (fc *FullContext) GetOmap(objs []*ObjectAddress, name string) ([]byte, error) {
	for _, obj := range objs {
		if osda, found := fc.OSDs[obj.osdnum]; !found {
			continue
		} else if ret, err := osda.GetOmap(obj, name); err != nil {
			continue
		} else {
			return ret, nil
		}
	}
	return nil, fmt.Errorf("not available")
}

func (fc *FullContext) GetData(objs []*ObjectAddress) ([]byte, error) {
	for _, obj := range objs {
		if osda, found := fc.OSDs[obj.osdnum]; !found {
			continue
		} else if ret, err := osda.GetData(obj); err != nil {
			continue
		} else {
			return ret, nil
		}
	}
	return nil, fmt.Errorf("not available")
}

func NewFullContext(stores []string) (*FullContext, error) {
	if pools, err := ReadPools(); err != nil {
		return nil, err
	} else if osdlist, err := ReadOSDs(); err != nil {
		return nil, err
	} else if mst, err := LoadMultiStore(stores); err != nil {
		return nil, err
	} else {
		return &FullContext{Pools: pools, OSDs: osdlist, Store: mst}, nil
	}
}

type VolumeListEntry struct {
	PoolId int64
	Name   string
	Id     int64
	Error  error
}

type VolumeParentRef struct {
	Pool    int64
	Id      int64
	SnapRef int64
}

func ParseParent(data []byte) (*VolumeParentRef, error) {
	pool_id := ByteSliceToInt64(data[6:14])
	pns_len := ByteSliceToInt(data[14:18])
	parentid_len := ByteSliceToInt(data[18+pns_len : 22+pns_len])
	parentid_str := string(data[22+pns_len : 22+pns_len+parentid_len])
	snapref := ByteSliceToInt64(data[22+pns_len+parentid_len : 30+pns_len+parentid_len])
	if parent_id, err := strconv.ParseInt(parentid_str, 16, 64); err != nil {
		return nil, err
	} else {
		return &VolumeParentRef{
			Pool:    pool_id,
			Id:      parent_id,
			SnapRef: snapref,
		}, nil
	}
}

func ListVolumes(poolid int64, ctx *FullContext) ([]*VolumeListEntry, error) {
	if ctx == nil {
		return nil, fmt.Errorf("null context")
	}
	ret := make([]*VolumeListEntry, 0, 1000)
	if objs, err := FindObject(ctx.Store, poolid, "rbd_directory", -1); err != nil {
		return nil, err
	} else if len(objs) == 0 {
		return nil, not_found
	} else if omaps, err := ctx.ListOmaps(objs); err != nil {
		return nil, err
	} else {
		for _, omap_name := range omaps {
			if !StartsWith(omap_name, "name_") {
				continue
			}
			vol_name := omap_name[5:]
			if volid_data, err := ctx.GetOmap(objs, omap_name); err != nil {
				ret = append(
					ret,
					&VolumeListEntry{
						PoolId: poolid,
						Name:   vol_name,
						Id:     -1,
						Error:  err,
					},
				)
			} else {
				volid_str := string(volid_data[4:])
				if volid, err := strconv.ParseInt(volid_str, 16, 64); err != nil {
					ret = append(
						ret,
						&VolumeListEntry{
							PoolId: poolid,
							Name:   vol_name,
							Id:     -1,
							Error:  err,
						},
					)
				} else {
					ret = append(
						ret,
						&VolumeListEntry{
							PoolId: poolid,
							Name:   vol_name,
							Id:     volid,
							Error:  nil,
						},
					)
				}
			}
		}
	}
	return ret, nil
}

func FindVolumeByName(vols []*VolumeListEntry, name string) (*VolumeListEntry, error) {
	for _, v := range vols {
		if v.Name == name {
			return v, nil
		}
	}
	return nil, not_found
}

func ReadVolumeChunks(src DataSource, ctx *FullContext, wt *WriteTask) error {
	if plan, err := GetPlan2(src, ctx); err != nil {
		return err
	} else {
		for _, pe := range plan {
			if len(pe.objects) > 0 {
				var data []byte
				if data, err = ctx.GetData(pe.objects); err != nil {
					break
				} else if err = wt.Write(data[pe.object_offset : pe.object_offset+pe.size]); err != nil {
					break
				}
			} else {
				wt.Write(make([]byte, pe.size))
			}
		}
		return err
	}
}

type WriteTask struct {
	File *os.File
}

func (wt *WriteTask) Write(data []byte) error {
	_, err := wt.File.Write(data)
	return err
}

func (wt *WriteTask) Close() error {
	return wt.File.Close()
}

func NewWriteTask(filename string) (*WriteTask, error) {
	if fd, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644); err == nil {
		return &WriteTask{
			File: fd,
		}, nil
	} else {
		return nil, err
	}
}

func ExportVolume(pool string, volume string, stores []string) {
	tokens := strings.Split(volume, "=")
	if len(tokens) != 2 {
		fmt.Println("Bad params")
		Usage()
		return
	}
	volume = tokens[0]
	target := tokens[1]
	if fctx, err := NewFullContext(stores); err != nil {
		fmt.Printf("error loading context: %s\n", err)
	} else if pool_id, found := fctx.Pools[pool]; !found {
		fmt.Printf("pool %s not found\n", pool)
	} else if vol_entry, err := GetVolumeByName(pool_id, volume, fctx); err != nil {
		fmt.Printf("error finding volume %s in pool (%d)%s: %s\n", volume, pool_id, pool, err)
	} else if vol, err := GetVolume(vol_entry, fctx); err != nil {
		fmt.Printf("error resolving volume %s in pool (%d)%s: %s\n", volume, pool_id, pool, err)
	} else {
		Debugf("found volume (%d)%s/%s\n", pool_id, pool, vol.name)
		if writer, err := NewWriteTask(target); err == nil {
			if err := ReadVolumeChunks(vol, fctx, writer); err != nil {
				fmt.Println("Error reading volume")
			} else {
				fmt.Println("Success reading volume")
			}
			writer.Close()
		} else {
			fmt.Print("Error opening output file", target)
		}
	}
}

func FindVolumeById(vols []*VolumeListEntry, id int64) (*VolumeListEntry, error) {
	for _, v := range vols {
		if v.Id == id {
			return v, nil
		}
	}
	return nil, not_found
}

func GetVolumeByName(poolid int64, name string, ctx *FullContext) (*VolumeListEntry, error) {
	if ctx == nil {
		return nil, fmt.Errorf("null context")
	}
	if vols, err := ListVolumes(poolid, ctx); err != nil {
		return nil, err
	} else {
		for _, e := range vols {
			if e.Name == name {
				return e, nil
			}
		}
	}
	return nil, not_found
}

func GetVolume(entry *VolumeListEntry, ctx *FullContext) (*Volume, error) {
	if ctx == nil {
		return nil, fmt.Errorf("null context")
	}
	if entry == nil {
		return nil, fmt.Errorf("null_entry error")
	}
	header := fmt.Sprintf("rbd_header.%x", entry.Id)
	if objlocs, err := FindObject(ctx.Store, entry.PoolId, header, -1); err != nil {
		return nil, err
	} else if all_omaps, err := ctx.ListOmaps(objlocs); err != nil {
		return nil, fmt.Errorf("can't list omaps for (%d)/%s - %s", entry.PoolId, header, err)
	} else if size_omap, err := ctx.GetOmap(objlocs, "size"); err != nil {
		return nil, fmt.Errorf("can't get size omap for (%d)/%s - %s", entry.PoolId, header, err)
	} else if order_omap, err := ctx.GetOmap(objlocs, "order"); err != nil {
		return nil, fmt.Errorf("can't get order omap for (%d)/%s - %s", entry.PoolId, header, err)
	} else if hdr_omaps, err := ctx.ListOmaps(objlocs); err != nil {
		return nil, fmt.Errorf("can't query snap omaps for (%d)/%s - %s", entry.PoolId, header, err)
	} else {
		ret := &Volume{
			id:        entry.Id,
			pool:      entry.PoolId,
			name:      entry.Name,
			size:      ByteSliceToInt64(size_omap),
			order:     int(order_omap[0]),
			Snapshots: map[string]*Snapshot{},
			Parent:    nil, // FIXME
		}
		for _, omap_name := range hdr_omaps {
			if !StartsWith(omap_name, "snapshot_") {
				continue
			}
			snap_ref, _ := strconv.ParseInt(omap_name[9:], 16, 64)
			if omap_val, err := ctx.GetOmap(objlocs, omap_name); err != nil {
				return nil, err
			} else {
				snap_name_len := ByteSliceToInt(omap_val[14:18])
				snap_name := string(omap_val[18 : 18+snap_name_len])
				snap_size := ByteSliceToInt64(omap_val[18+snap_name_len : 18+snap_name_len+8])
				ret.Snapshots[snap_name] = &Snapshot{
					Volume:   ret,
					name:     snap_name,
					size:     snap_size,
					snap_ref: snap_ref,
				}
			}
		}
		if slices.Contains(all_omaps, "parent") {
			if pdata, err := ctx.GetOmap(objlocs, "parent"); err == nil {
				if parent, err := ParseParent(pdata); err != nil {
					return nil, err
				} else {
					if vollist, err := ListVolumes(parent.Pool, ctx); err != nil {
						return nil, fmt.Errorf("can't list volumes via (%d)/rbd_directory - %s", parent.Pool, err)
					} else if vle, err := FindVolumeById(vollist, parent.Id); err != nil {
						return nil, fmt.Errorf("can't find rbd_header.%x volumes via (%d)/rbd_directory - %s", parent.Id, parent.Pool, err)
					} else if pvol, err := GetVolume(vle, ctx); err != nil {
						return nil, fmt.Errorf("can't build parent for (%d)/(rbd_header.%x) - %s", entry.PoolId, parent.Id, err)
					} else if snap, err := pvol.GetSnapByRef(parent.SnapRef); err != nil {
						return nil, fmt.Errorf("can't find parent snap (%d)/(rbd_header.%x)#%x - %s", parent.Pool, parent.Id, parent.SnapRef, err)
					} else {
						ret.Parent = snap
					}
				}
			} else {
				return nil, err
			}
		}
		return ret, nil
	}
}

func QueryVolume(poolname string, rbdname string, stores []string) {
	if ctx, err := NewFullContext(stores); err != nil {
		fmt.Printf("error getting full cluster context: %s\n", err)
	} else if poolid, found := ctx.Pools[poolname]; !found {
		fmt.Printf("pool %s not found\n", poolname)
	} else if vollist, err := ListVolumes(poolid, ctx); err != nil {
		fmt.Printf("volume list for pool (%d)%s failed\n", poolid, poolname)
	} else if entry, err := FindVolumeByName(vollist, rbdname); err != nil {
		fmt.Printf("volume %s/%s not found (%s)\n", poolname, rbdname, err)
	} else if vol, err := GetVolume(entry, ctx); err != nil {
		fmt.Printf("error getting volume %s/%s - %s\n", poolname, rbdname, err)
	} else {
		fmt.Printf("RBD %s/%s\n", poolname, rbdname)
		fmt.Printf("   id %x\n", vol.id)
		fmt.Printf("   size %d\n", vol.size)
		fmt.Printf("   order %d (%d bytes)\n", vol.order, vol.BlockSize())
		for n, i := range vol.Snapshots {
			fmt.Printf("   snapshot %s id %x size %d\n", n, i.snap_ref, i.size)
		}
		if vol.Parent != nil {
			fmt.Printf(
				"   parent (%d)/%s#%x (rbd_header.%x)\n",
				vol.Parent.Volume.pool,
				vol.Parent.Volume.name,
				vol.Parent.snap_ref,
				vol.Parent.Volume.id,
			)
		}
	}
}

func ShowListVolumes(poolname string, stores []string) {
	if ctx, err := NewFullContext(stores); err != nil {
		fmt.Printf("error getting full cluster context: %s\n", err)
	} else if poolid, found := ctx.Pools[poolname]; !found {
		fmt.Printf("pool %s not found\n", poolname)
	} else if vols, err := ListVolumes(poolid, ctx); err != nil {
		fmt.Printf("Error listing pool %s volumes: %s\n", poolname, err)
	} else {
		for _, v := range vols {
			if v.Error == nil {
				fmt.Printf("volume '%s' id '%x'\n", v.Name, v.Id)
			} else {
				fmt.Printf("volume '%s' id '%x' error '%s'\n", v.Name, v.Id, v.Error)
			}
		}
	}
}

func Usage() {
	fmt.Println("Usage:")
	fmt.Printf("  %s scan <store> <osd_id>=<mountpath> [ ... <osd_id>=<mount_path> ]\n", os.Args[0])
	fmt.Printf("  %s serve [<ip>:]<port>=<mountpath> [ ... [<ip>:]<port>=<mount_path> ]\n", os.Args[0])
	fmt.Printf("  %s list_volumes <pool> <store> [ ... <store> ]\n", os.Args[0])
	fmt.Printf("  %s info <pool> <volume> <store> [ ... <store> ]\n", os.Args[0])
	fmt.Printf("  %s extract <pool> <volume>=<target_path> <store> [ ... <store> ]\n", os.Args[0])
}

func main() {
	is_debug = strings.ToLower(os.Getenv("SHOW_DEBUG")) != ""
	if len(os.Args) < 3 {
		Usage()
	} else if os.Args[1] == "scan" && len(os.Args) > 3 {
		if objstore, err := CreateObjectStore(os.Args[2]); err != nil {
			fmt.Println("Error creating objectstore", os.Args[2], ":", err)
			return
		} else {
			run_scan(objstore, os.Args[3:])
		}
	} else if os.Args[1] == "serve" && len(os.Args) > 3 {
		run_servers(os.Args[2:])
	} else if os.Args[1] == "list_volumes" && len(os.Args) > 3 {
		ShowListVolumes(os.Args[2], os.Args[3:])
	} else if os.Args[1] == "info" && len(os.Args) > 3 {
		QueryVolume(os.Args[2], os.Args[3], os.Args[4:])
	} else if os.Args[1] == "extract" && len(os.Args) > 3 {
		ExportVolume(os.Args[2], os.Args[3], os.Args[4:])
	} else {
		Usage()
	}
}
