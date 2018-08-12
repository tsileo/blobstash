package lua // import "a4.io/blobstash/pkg/gitserver/lua"

import (
	"github.com/xeonx/timeago"
	"github.com/yuin/gopher-lua"
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
		"name":    repoName,
		"summary": repoSummary,
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

func convertCommit(L *lua.LState, commit *object.Commit) *lua.LTable {
	tbl := L.CreateTable(0, 4)
	tbl.RawSetH(lua.LString("time_ago"), lua.LString(timeago.English.Format(commit.Committer.When)))
	tbl.RawSetH(lua.LString("message"), lua.LString(commit.Message))
	tbl.RawSetH(lua.LString("author_name"), lua.LString(commit.Committer.Name))
	tbl.RawSetH(lua.LString("author_email"), lua.LString(commit.Committer.Email))
	return tbl
}
func convertRefSummary(L *lua.LState, refSummary *gitserver.RefSummary) *lua.LTable {
	tbl := L.CreateTable(0, 5)
	tbl.RawSetH(lua.LString("commit_time_ago"), lua.LString(timeago.English.Format(refSummary.Commit.Committer.When)))
	tbl.RawSetH(lua.LString("commit_message"), lua.LString(refSummary.Commit.Message))
	tbl.RawSetH(lua.LString("commit_author_name"), lua.LString(refSummary.Commit.Committer.Name))
	tbl.RawSetH(lua.LString("commit_author_email"), lua.LString(refSummary.Commit.Committer.Email))
	tbl.RawSetH(lua.LString("ref_short_name"), lua.LString(refSummary.Ref.Name().Short()))
	return tbl
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
	branchesTbl := L.CreateTable(len(summary.Branches), 0)
	for _, refSummary := range summary.Branches {
		branchesTbl.Append(convertRefSummary(L, refSummary))
	}
	tagsTbl := L.CreateTable(len(summary.Tags), 0)
	for _, refSummary := range summary.Tags {
		tagsTbl.Append(convertRefSummary(L, refSummary))
	}
	commitsTbl := L.CreateTable(len(summary.Commits), 0)
	for _, commit := range summary.Commits {
		commitsTbl.Append(convertCommit(L, commit))
	}
	tbl := L.CreateTable(0, 4)
	tbl.RawSetH(lua.LString("branches"), branchesTbl)
	tbl.RawSetH(lua.LString("tags"), tagsTbl)
	tbl.RawSetH(lua.LString("commits"), commitsTbl)
	tbl.RawSetH(lua.LString("commits_count"), lua.LNumber(summary.CommitsCount))
	L.Push(tbl)
	return 1
}
