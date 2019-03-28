package lua // import "a4.io/blobstash/pkg/filetree/lua"

import (
	"context"
	"fmt"
	"os"
	"strings"

	humanize "github.com/dustin/go-humanize"
	"github.com/yuin/gopher-lua"

	"a4.io/blobsfile"
	"a4.io/blobstash/pkg/filetree"
	rnode "a4.io/blobstash/pkg/filetree/filetreeutil/node"
	"a4.io/blobstash/pkg/filetree/imginfo"
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

func convertNode(L *lua.LState, ft *filetree.FileTree, node *filetree.Node) *lua.LTable {
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
		childrenTbl.Append(convertNode(L, ft, child))
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

func setupFileTree(ft *filetree.FileTree, bs store.BlobStore) func(*lua.LState) int {
	return func(L *lua.LState) int {
		// register functions to the table
		mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
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
				L.Push(convertNode(L, ft, node))
				return 1

			},
			"fs": func(L *lua.LState) int {
				fs := filetree.NewFS(L.ToString(1), ft)
				node, _, _, err := fs.Path(context.TODO(), "/", 1, false, 0)
				if err != nil {
					panic(err)
				}
				L.Push(convertNode(L, ft, node))
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
				pathTable := L.CreateTable(len(path), 0)
				for _, nodeInfo := range path {
					pathTable.Append(buildFSInfo(L, nodeInfo.Name, nodeInfo.Ref, ""))
				}
				L.Push(convertNode(L, ft, node))
				L.Push(pathTable)
				return 2
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
		})
		// returns the module
		L.Push(mod)
		return 1
	}
}

// Setup loads the filetree Lua module
func Setup(L *lua.LState, ft *filetree.FileTree, bs store.BlobStore) {
	L.PreloadModule("filetree", setupFileTree(ft, bs))
}
