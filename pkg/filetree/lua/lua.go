package lua // import "a4.io/blobstash/pkg/filetree/lua"

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/yuin/gopher-lua"
	"gopkg.in/src-d/go-git.v4/utils/binary"

	"a4.io/blobsfile"
	"a4.io/blobstash/pkg/filetree"
	rnode "a4.io/blobstash/pkg/filetree/filetreeutil/node"
	"a4.io/blobstash/pkg/filetree/imginfo"
	"a4.io/blobstash/pkg/filetree/reader/filereader"
	"a4.io/blobstash/pkg/filetree/vidinfo"
	"a4.io/blobstash/pkg/filetree/writer"
	"a4.io/blobstash/pkg/stash/store"
)

func buildFSInfo(L *lua.LState, name, ref, tgzURL string) *lua.LTable {
	tbl := L.CreateTable(0, 2)
	tbl.RawSetString("name", lua.LString(name))
	tbl.RawSetString("ref", lua.LString(ref))
	tbl.RawSetString("tgz_url", lua.LString(tgzURL))
	return tbl
}

func convertNode(L *lua.LState, ft *filetree.FileTree, bs store.BlobStore, node *filetree.Node) *lua.LTable {
	tbl := L.CreateTable(0, 32)
	dlURL, embedURL, err := ft.GetSemiPrivateLink(node)
	if err != nil {
		panic(err)
	}
	tbl.RawSetString("url", lua.LString(embedURL))
	tbl.RawSetString("dl_url", lua.LString(dlURL))
	if vidinfo.IsVideo(node.Name) {
		if node.Info != nil && node.Info.Video != nil {
			tbl.RawSetString("video_width", lua.LNumber(node.Info.Video.Width))
			tbl.RawSetString("video_height", lua.LNumber(node.Info.Video.Height))
			tbl.RawSetString("video_codec", lua.LString(node.Info.Video.Codec))
			t := node.Info.Video.Duration
			tbl.RawSetString("video_duration", lua.LString(fmt.Sprintf("%02d:%02d:%02d", (t/3600), (t/60)%60, t%60)))
		}
		webmURL, webmPosterURL, err := ft.GetWebmLink(node)
		if err != nil {
			panic(err)
		}
		tbl.RawSetString("is_video", lua.LTrue)
		tbl.RawSetString("webm_poster_url", lua.LString(webmPosterURL))
		tbl.RawSetString("webm_url", lua.LString(webmURL))
	} else {
		tbl.RawSetString("is_video", lua.LFalse)
		tbl.RawSetString("webm_poster_url", lua.LString(""))
		tbl.RawSetString("webm_url", lua.LString(""))
	}
	if imginfo.IsImage(node.Name) {
		tbl.RawSetString("is_image", lua.LTrue)
		if node.Info != nil && node.Info.Image != nil {
			tbl.RawSetString("image_width", lua.LNumber(node.Info.Image.Width))
			tbl.RawSetString("image_height", lua.LNumber(node.Info.Image.Height))
			// TODO(tsileo): export EXIF
		}
	} else {
		tbl.RawSetString("is_image", lua.LFalse)
	}
	if node.Size < 1024*1024 {
		f := filereader.NewFile(context.TODO(), bs, node.Meta, nil)
		defer f.Close()
		contents, err := ioutil.ReadAll(f)
		if err != nil {
			panic(err)
		}
		isBinary, err := binary.IsBinary(bytes.NewReader(contents))
		if err != nil {
			panic(err)
		}
		if !isBinary {
			tbl.RawSetString("contents", lua.LString(contents))
		}
	} else {
		tbl.RawSetString("contents", lua.LString(""))
	}
	tbl.RawSetString("hash", lua.LString(node.Hash))
	tbl.RawSetString("name", lua.LString(node.Name))
	tbl.RawSetString("type", lua.LString(node.Type))
	tbl.RawSetString("mtime", lua.LString(node.ModTime))
	tbl.RawSetString("mtime_short", lua.LString(strings.Split(node.ModTime, "T")[0]))
	tbl.RawSetString("citme", lua.LString(node.ChangeTime))
	tbl.RawSetString("mode", lua.LString(os.FileMode(node.Mode).String()))
	tbl.RawSetString("size", lua.LNumber(node.Size))
	tbl.RawSetString("size_human", lua.LString(humanize.Bytes(uint64(node.Size))))
	childrenTbl := L.CreateTable(len(node.Children), 0)
	for _, child := range node.Children {
		childrenTbl.Append(convertNode(L, ft, bs, child))
	}
	tbl.RawSetString("children", childrenTbl)
	if node.Type == rnode.Dir {
		tgzURL, err := ft.GetTgzLink(node)
		if err != nil {
			panic(err)
		}

		tbl.RawSetString("tgz_url", lua.LString(tgzURL))
		tbl.RawSetString("children_count", lua.LNumber(node.ChildrenCount))
	}
	return tbl
}

func setupFileTree(ft *filetree.FileTree, bs store.BlobStore, kv store.KvStore) func(*lua.LState) int {
	return func(L *lua.LState) int {
		// register functions to the table
		mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
			"create_fs": func(L *lua.LState) int {
				node, err := ft.CreateFS(context.TODO(), L.ToString(1), filetree.FSKeyFmt)
				if err != nil {
					panic(err)
				}
				L.Push(convertNode(L, ft, bs, node))
				return 1
			},
			"iter_fs": func(L *lua.LState) int {
				it, err := ft.IterFS(context.TODO(), "")
				if err != nil {
					panic(err)
				}
				tbl := L.CreateTable(len(it), 0)
				for _, kv := range it {
					tgzURL, err := ft.GetTgzLink(&filetree.Node{Hash: kv.Ref})
					if err != nil {
						panic(err)
					}
					tbl.Append(buildFSInfo(L, kv.Name, kv.Ref, tgzURL))
				}
				L.Push(tbl)
				return 1
			},
			"fs_versions": func(L *lua.LState) int {
				versions, err := ft.LuaFSVersions(L.ToString(1))
				if err != nil {
					panic(err)
				}

				tbl := L.CreateTable(len(versions), 0)
				for _, v := range versions {
					snap := L.CreateTable(0, 4)
					snap.RawSetString("ref", lua.LString(v.Ref))
					snap.RawSetString("hostname", lua.LString(v.Hostname))
					snap.RawSetString("message", lua.LString(v.Message))
					snap.RawSetString("created_at", lua.LString(time.Unix(0, v.CreatedAt).Format(time.RFC3339)))
					tbl.Append(snap)
				}
				L.Push(tbl)
				return 1
			},
			"fs_by_name": func(L *lua.LState) int {
				fs, err := ft.FS(context.TODO(), L.ToString(1), filetree.FSKeyFmt, false, 0)
				if err != nil {
					panic(err)
				}
				node, _, _, err := fs.Path(context.TODO(), "/", 1, false, 0)
				if err != nil {
					if err == blobsfile.ErrBlobNotFound {
						L.Push(lua.LNil)
						return 1
					}

					panic(err)
				}
				L.Push(convertNode(L, ft, bs, node))
				return 1

			},
			"fs_by_name_at": func(L *lua.LState) int {
				fs, err := ft.FS(context.TODO(), L.ToString(1), filetree.FSKeyFmt, false, 0)
				if err != nil {
					panic(err)
				}
				node, _, _, err := fs.Path(context.TODO(), L.ToString(2), 1, false, 0)
				if err != nil {
					if err == blobsfile.ErrBlobNotFound {
						L.Push(lua.LNil)
						return 1
					}

					panic(err)
				}
				L.Push(convertNode(L, ft, bs, node))
				return 1

			},
			"fs": func(L *lua.LState) int {
				fs := filetree.NewFS(L.ToString(1), ft)
				node, _, _, err := fs.Path(context.TODO(), "/", 1, false, 0)
				if err != nil {
					panic(err)
				}
				L.Push(convertNode(L, ft, bs, node))
				return 1
			},
			"node": func(L *lua.LState) int {
				node, err := ft.NodeWithChildren(context.TODO(), L.ToString(2))
				if err != nil {
					panic(err)
				}
				path, err := ft.BruteforcePath(context.TODO(), L.ToString(1), L.ToString(2))
				if err != nil {
					panic(err)
				}
				spath := ""
				pathTable := L.CreateTable(len(path), 0)
				for _, nodeInfo := range path {
					if nodeInfo.Name != "_root" {
						spath = spath + "/" + nodeInfo.Name
					}
					pathTable.Append(buildFSInfo(L, nodeInfo.Name, nodeInfo.Ref, ""))
				}
				if node.Name != "_root" {
					spath = spath + "/" + node.Name
				}
				L.Push(convertNode(L, ft, bs, node))
				L.Push(pathTable)
				L.Push(lua.LString(spath))
				return 3
			},
			"put_file": func(L *lua.LState) int {
				uploader := writer.NewUploader(filetree.NewBlobStoreCompat(bs, context.TODO()))
				name := L.ToString(1)
				newName := L.ToString(2)
				extraMeta := L.ToBool(3)
				var ref string
				if newName != "" {
					// Upload the given file with a new name and without meta data (mtime/ctime/mode)
					node, err := uploader.PutFileRename(name, newName, extraMeta)
					if err != nil {
						panic(err)
					}
					ref = node.Hash
				} else {
					node, err := uploader.PutFile(name)
					if err != nil {
						panic(err)
					}
					ref = node.Hash
				}
				L.Push(lua.LString(ref))
				return 1
			},
			"mkdir": func(L *lua.LState) int {
				ctx := context.TODO()
				fs, err := ft.FS(ctx, L.ToString(1), filetree.FSKeyFmt, false, 0)
				if err != nil {
					panic(err)
				}
				node, err := fs.Mkdir(ctx, filetree.FSKeyFmt, L.ToString(2), L.ToString(3))
				if err != nil {
					panic(err)
				}
				L.Push(lua.LString(node.Hash))
				return 1
			},
			"put_file_at": func(L *lua.LState) int {
				uploader := writer.NewUploader(filetree.NewBlobStoreCompat(bs, context.TODO()))
				name := L.ToString(1)
				contents := L.ToString(2)
				var ref string
				node, err := uploader.PutReader(name, strings.NewReader(contents), nil)
				if err != nil {
					panic(err)
				}

				ref = node.Hash
				ctx := context.TODO()

				fs, err := ft.FS(ctx, L.ToString(3), filetree.FSKeyFmt, false, 0)
				if err != nil {
					panic(err)
				}

				t := time.Now().Unix()
				parentNode, _, _, err := fs.Path(ctx, L.ToString(4), 1, true, t)
				if err != nil {
					panic(err)
				}
				if parentNode.Type != rnode.Dir {
					panic("only dir can be patched")
				}

				newParent, _, err := ft.AddChild(ctx, parentNode, node, filetree.FSKeyFmt, t)
				if err != nil {
					panic(err)
				}

				fmt.Printf("newParent=%+v\nnew file ref=%s\n", newParent, ref)
				L.Push(lua.LString(newParent.Hash))
				L.Push(lua.LString(ref))
				return 2
			},
		})
		// returns the module
		L.Push(mod)
		return 1
	}
}

// Setup loads the filetree Lua module
func Setup(L *lua.LState, ft *filetree.FileTree, bs store.BlobStore, kv store.KvStore) {
	L.PreloadModule("filetree", setupFileTree(ft, bs, kv))
}
