package lua // import "a4.io/blobstash/pkg/gitserver/lua"

import (
	"os"
	"time"

	"github.com/xeonx/timeago"
	"github.com/yuin/gopher-lua"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"

	"a4.io/blobstash/pkg/apps/luautil"
	"a4.io/blobstash/pkg/gitserver"
)

func setupGitServer(gs *gitserver.GitServer) func(*lua.LState) int {
	return func(L *lua.LState) int {
		// register functions to the table
		mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
			"repo": func(L *lua.LState) int {
				ns := L.ToString(1)
				name := L.ToString(2)
				ud := L.NewUserData()
				ud.Value = &repo{gs, ns, name}
				L.SetMetatable(ud, L.GetTypeMetatable("repo"))
				L.Push(ud)
				return 1
			},
			"namespaces": func(L *lua.LState) int {
				namespaces, err := gs.Namespaces()
				if err != nil {
					panic(err)
				}
				L.Push(luautil.InterfaceToLValue(L, namespaces))
				return 1
			},
			"repositories": func(L *lua.LState) int {
				ns := L.ToString(1)
				repos, err := gs.Repositories(ns)
				if err != nil {
					panic(err)
				}
				L.Push(luautil.InterfaceToLValue(L, repos))
				return 1
			},
		})
		// returns the module
		L.Push(mod)
		return 1
	}
}

func Setup(L *lua.LState, gs *gitserver.GitServer) {
	mtCol := L.NewTypeMetatable("repo")
	L.SetField(mtCol, "__index", L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"name":       repoName,
		"refs":       repoRefs,
		"summary":    repoSummary,
		"tree":       repoTree,
		"log":        repoLog,
		"get_commit": repoGetCommit,
		"get_tree":   repoGetTree,
		"get_file":   repoGetFile,
	}))
	L.PreloadModule("gitserver", setupGitServer(gs))
}

type repo struct {
	gs       *gitserver.GitServer
	ns, name string
}

func checkRepo(L *lua.LState) *repo {
	ud := L.CheckUserData(1)
	if v, ok := ud.Value.(*repo); ok {
		return v
	}
	L.ArgError(1, "repo expected")
	return nil
}

func repoName(L *lua.LState) int {
	repo := checkRepo(L)
	if repo == nil {
		return 0
	}
	L.Push(lua.LString(repo.name))
	return 1
}

func convertFile(L *lua.LState, file *object.File) *lua.LTable {
	tbl := L.CreateTable(0, 3)
	var contents string
	var err error
	isBinary := lua.LTrue
	gisBinary, err := file.IsBinary()
	if err != nil {
		panic(err)
	}
	if !gisBinary {
		isBinary = lua.LFalse
		contents, err = file.Contents()
		if err != nil {
			panic(err)
		}

	}
	tbl.RawSetH(lua.LString("hash"), lua.LString(file.Name))
	tbl.RawSetH(lua.LString("contents"), lua.LString(contents))
	tbl.RawSetH(lua.LString("is_binary"), isBinary)
	return tbl
}

func mustParse(d string) time.Time {
	t, err := time.Parse(time.RFC3339, d)
	if err != nil {
		panic(err)
	}
	return t
}

func convertCommit(L *lua.LState, commit *gitserver.GitServerCommit, withPatch bool) *lua.LTable {
	cntWithPatch := 0
	if withPatch {
		cntWithPatch = 3
	}
	tbl := L.CreateTable(0, 12+cntWithPatch)
	tbl.RawSetH(lua.LString("hash"), lua.LString(commit.Hash))
	tbl.RawSetH(lua.LString("message"), lua.LString(commit.Message))
	// Author
	tbl.RawSetH(lua.LString("author_time_ago"), lua.LString(timeago.English.Format(mustParse(commit.Author.Date))))
	tbl.RawSetH(lua.LString("author_time"), lua.LString(commit.Author.Date))
	tbl.RawSetH(lua.LString("author_name"), lua.LString(commit.Author.Name))
	tbl.RawSetH(lua.LString("author_email"), lua.LString(commit.Author.Email))
	// Comitter
	tbl.RawSetH(lua.LString("comitter_time_ago"), lua.LString(timeago.English.Format(mustParse(commit.Committer.Date))))
	tbl.RawSetH(lua.LString("comitter_time"), lua.LString(commit.Committer.Date))
	tbl.RawSetH(lua.LString("comitter_name"), lua.LString(commit.Committer.Name))
	tbl.RawSetH(lua.LString("comitter_email"), lua.LString(commit.Committer.Email))

	tbl.RawSetH(lua.LString("tree_hash"), lua.LString(commit.Tree))
	rcommit := commit.Raw
	if len(rcommit.ParentHashes) > 0 {
		tbl.RawSetH(lua.LString("parent_hash"), lua.LString(rcommit.ParentHashes[0].String()))
	}
	if withPatch && len(rcommit.ParentHashes) > 0 {
		ci := rcommit.Parents()
		defer ci.Close()
		parentCommit, err := ci.Next()
		if err != nil {
			panic(err)
		}
		parentTree, err := parentCommit.Tree()
		if err != nil {
			panic(err)
		}
		tree, err := rcommit.Tree()
		if err != nil {
			panic(err)
		}
		changes, err := parentTree.Diff(tree)
		if err != nil {
			panic(err)
		}
		patch, err := changes.Patch()
		if err != nil {
			panic(err)
		}
		var filesChanged, additions, deletions int
		stats := patch.Stats()
		lfilestats := L.CreateTable(len(stats), 0)
		for _, fstat := range stats {
			filesChanged++
			additions += fstat.Addition
			deletions += fstat.Deletion
			lfilestats.Append(newFileStat(L, fstat.Name, fstat.Addition, fstat.Deletion))
		}
		lstats := L.CreateTable(0, 3)
		lstats.RawSetH(lua.LString("files_changed"), lua.LNumber(filesChanged))
		lstats.RawSetH(lua.LString("additions"), lua.LNumber(additions))
		lstats.RawSetH(lua.LString("deletions"), lua.LNumber(deletions))

		tbl.RawSetH(lua.LString("stats"), lstats)
		tbl.RawSetH(lua.LString("file_stats"), lfilestats)
		tbl.RawSetH(lua.LString("patch"), lua.LString(patch.String()))
	}

	return tbl
}

func newFileStat(L *lua.LState, name string, additions, deletions int) *lua.LTable {
	stats := L.CreateTable(0, 3)
	stats.RawSetH(lua.LString("name"), lua.LString(name))
	stats.RawSetH(lua.LString("additions"), lua.LNumber(additions))
	stats.RawSetH(lua.LString("deletions"), lua.LNumber(deletions))
	return stats
}

func convertRefSummary(L *lua.LState, refSummary *gitserver.RefSummary) *lua.LTable {
	tbl := L.CreateTable(0, 7)
	tbl.RawSetH(lua.LString("commit_time_ago"), lua.LString(timeago.English.Format(mustParse(refSummary.Commit.Committer.Date))))
	tbl.RawSetH(lua.LString("commit_short_hash"), lua.LString(refSummary.Commit.Hash[:8]))
	tbl.RawSetH(lua.LString("commit_hash"), lua.LString(refSummary.Commit.Hash))
	tbl.RawSetH(lua.LString("commit_message"), lua.LString(refSummary.Commit.Message))
	tbl.RawSetH(lua.LString("commit_author_name"), lua.LString(refSummary.Commit.Committer.Name))
	tbl.RawSetH(lua.LString("commit_author_email"), lua.LString(refSummary.Commit.Committer.Email))
	tbl.RawSetH(lua.LString("ref_short_name"), lua.LString(refSummary.Ref))
	return tbl
}

func convertTreeEntry(L *lua.LState, treeEntry *object.TreeEntry) *lua.LTable {
	tbl := L.CreateTable(0, 4)
	tbl.RawSetH(lua.LString("name"), lua.LString(treeEntry.Name))
	isFile := lua.LFalse
	mode := os.FileMode(treeEntry.Mode)
	if treeEntry.Mode.IsFile() {
		isFile = lua.LTrue
	} else {
		mode = mode | os.ModeDir
	}
	tbl.RawSetH(lua.LString("mode"), lua.LString(mode.String()))
	tbl.RawSetH(lua.LString("is_file"), isFile)
	tbl.RawSetH(lua.LString("hash"), lua.LString(treeEntry.Hash.String()))
	return tbl
}

func repoRefs(L *lua.LState) int {
	repo := checkRepo(L)
	if repo == nil {
		return 0
	}
	summary, err := repo.gs.RepoRefs(repo.ns, repo.name)
	if err != nil {
		panic(err)
	}
	branchesTbl := L.CreateTable(len(summary.Branches), 0)
	for _, refSummary := range summary.Branches {
		branchesTbl.Append(convertRefSummary(L, refSummary))
	}
	tagsTbl := L.CreateTable(len(summary.Tags), 0)
	for _, refSummary := range summary.Tags {
		tagsTbl.Append(convertRefSummary(L, refSummary))
	}
	tbl := L.CreateTable(0, 2)
	tbl.RawSetH(lua.LString("branches"), branchesTbl)
	tbl.RawSetH(lua.LString("tags"), tagsTbl)
	L.Push(tbl)
	return 1
}

func repoSummary(L *lua.LState) int {
	repo := checkRepo(L)
	if repo == nil {
		return 0
	}
	summary, err := repo.gs.RepoSummary(repo.ns, repo.name)
	if err != nil {
		panic(err)
	}
	if len(summary.Commits) > 3 {
		summary.Commits = summary.Commits[0:3]
	}
	commitsTbl := L.CreateTable(len(summary.Commits), 0)
	for _, commit := range summary.Commits {
		commitsTbl.Append(convertCommit(L, commit, false))
	}
	tbl := L.CreateTable(0, 2)
	tbl.RawSetH(lua.LString("readme"), lua.LString(summary.Readme))
	tbl.RawSetH(lua.LString("commits"), commitsTbl)
	L.Push(tbl)
	return 1
}

func repoLog(L *lua.LState) int {
	repo := checkRepo(L)
	if repo == nil {
		return 0
	}
	commits, err := repo.gs.RepoLog(repo.ns, repo.name)
	if err != nil {
		panic(err)
	}
	tbl := L.CreateTable(len(commits), 0)
	for _, commit := range commits {
		tbl.Append(convertCommit(L, commit, false))
	}
	L.Push(tbl)
	return 1

}

func repoTree(L *lua.LState) int {
	repo := checkRepo(L)
	if repo == nil {
		return 0
	}
	tree, err := repo.gs.RepoTree(repo.ns, repo.name)
	if err != nil {
		panic(err)
	}
	tbl := L.CreateTable(len(tree.Entries), 0)
	for _, entry := range tree.Entries {
		tbl.Append(convertTreeEntry(L, &entry))
	}
	L.Push(tbl)
	return 1

}
func repoGetTree(L *lua.LState) int {
	repo := checkRepo(L)
	if repo == nil {
		return 0
	}
	tree, err := repo.gs.RepoGetTree(repo.ns, repo.name, L.ToString(2))
	if err != nil {
		panic(err)
	}
	tbl := L.CreateTable(len(tree.Entries), 0)
	for _, entry := range tree.Entries {
		tbl.Append(convertTreeEntry(L, &entry))
	}
	L.Push(tbl)
	return 1
}

func repoGetCommit(L *lua.LState) int {
	repo := checkRepo(L)
	if repo == nil {
		return 0
	}
	hash := plumbing.NewHash(L.ToString(2))
	commit, err := repo.gs.RepoCommit(repo.ns, repo.name, hash)
	if err != nil {
		panic(err)
	}
	L.Push(convertCommit(L, commit, true))
	return 1
}
func repoGetFile(L *lua.LState) int {
	repo := checkRepo(L)
	if repo == nil {
		return 0
	}
	hash := plumbing.NewHash(L.ToString(2))
	file, err := repo.gs.RepoGetFile(repo.ns, repo.name, hash)
	if err != nil {
		panic(err)
	}
	L.Push(convertFile(L, file))
	return 1
}
