package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/branchsync"
	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/gate"
	"github.com/kunchenguid/no-mistakes/internal/ipc"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

func TestTriggerProofRunRejectsStaleBindingBeforeConcurrentRemoval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	work := t.TempDir()
	p := paths.WithRoot(makeSocketSafeTempDir(t))
	t.Setenv("NM_HOME", p.Root())
	if err := p.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	d, err := db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	cliGit(t, work, "init", "-b", "main")
	cliGit(t, work, "config", "user.name", "Test")
	cliGit(t, work, "config", "user.email", "test@example.com")
	cliGit(t, work, "commit", "--allow-empty", "-m", "base")
	base := cliGit(t, work, "rev-parse", "HEAD")
	writeCommit := func(name, content string) string {
		t.Helper()
		if err := os.WriteFile(filepath.Join(work, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		cliGit(t, work, "add", name)
		cliGit(t, work, "commit", "-m", name)
		return cliGit(t, work, "rev-parse", "HEAD")
	}
	privateHead := writeCommit("private.txt", "private\n")
	cliGit(t, work, "reset", "--hard", base)
	cliGit(t, work, "commit", "--allow-empty", "-m", "planned")
	plannedHead := cliGit(t, work, "rev-parse", "HEAD")
	cliGit(t, work, "reset", "--hard", base)
	cliGit(t, work, "commit", "--allow-empty", "-m", "planned-rewritten")
	candidate := writeCommit("candidate.txt", "candidate\n")
	branch := "feature/reconcile"
	nonce := "proof~1"

	repo, err := d.InsertRepo(work, "https://example.com/repo.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	gateDir := p.RepoDir(repo.ID)
	cliGit(t, "", "init", "--bare", gateDir)
	cliGit(t, gateDir, "config", "receive.advertisePushOptions", "true")
	cliGit(t, work, "remote", "add", gate.RemoteName, gateDir)
	cliGit(t, work, "push", gateDir, plannedHead+":refs/heads/"+branch)
	cliGit(t, work, "push", gateDir, privateHead+":refs/no-mistakes/test/private")
	privateArchive := "refs/tags/no-mistakes-abandoned/" + branch + "/" + privateHead
	cliGit(t, gateDir, "update-ref", privateArchive, privateHead)
	if _, err := gate.ApplyProofBranchReconciliation(ctx, gateDir, gate.StaleBranchPlan{
		Reconcile: true, Branch: branch, BranchRef: "refs/no-mistakes/test/private",
		PreviousHead: privateHead, ArchiveTag: privateArchive,
	}, candidate, nonce); err != nil {
		t.Fatal(err)
	}

	postReceiveMarker := filepath.Join(gateDir, "proof-pushed")
	postReceive := "#!/bin/sh\n: > '" + filepath.ToSlash(postReceiveMarker) + "'\n"
	if err := os.WriteFile(filepath.Join(gateDir, "hooks", "post-receive"), []byte(postReceive), 0o755); err != nil {
		t.Fatal(err)
	}

	srv := ipc.NewServer()
	srv.Handle(ipc.MethodGetActiveRun, func(context.Context, json.RawMessage) (interface{}, error) {
		return &ipc.GetActiveRunResult{}, nil
	})
	var probeCalls atomic.Int32
	srv.Handle(ipc.MethodProbeProofReconciliation, func(context.Context, json.RawMessage) (interface{}, error) {
		probeCalls.Add(1)
		cmd := exec.Command("git", "-C", gateDir, "update-ref", "-d", "refs/heads/"+branch, plannedHead)
		if output, err := cmd.CombinedOutput(); err != nil {
			return nil, fmt.Errorf("remove planned head during capability probe: %w: %s", err, output)
		}
		return &ipc.ProbeProofReconciliationResult{OK: true}, nil
	})
	srv.Handle(ipc.MethodClaimLaunchReceipt, func(context.Context, json.RawMessage) (interface{}, error) {
		return &ipc.ClaimLaunchReceiptResult{Receipt: &ipc.LaunchReceipt{
			RunID: "unexpected-run", LaunchNonce: nonce, ValidationGeneration: "generation", SubmittedHeadSHA: candidate,
		}}, nil
	})
	done := make(chan error, 1)
	go func() { done <- srv.Serve(p.Socket()) }()
	t.Cleanup(func() { srv.Close(); <-done })
	var client *ipc.Client
	deadline := time.Now().Add(3 * time.Second)
	for {
		client, err = ipc.Dial(p.Socket())
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer client.Close()

	chdir(t, work)
	env := &axiEnv{p: p, d: d, repo: repo, cfg: config.DefaultGlobalConfig(), client: client}
	receipt, err := triggerProofRun(ctx, env, branch, candidate, nil, "validate reconstructed work", "", false, nonce, "generation")
	if err == nil || !strings.Contains(err.Error(), "provenance does not match the planned head") || receipt != nil {
		t.Fatalf("want stale-provenance refusal without receipt, got receipt=%+v err=%v", receipt, err)
	}
	if calls := probeCalls.Load(); calls != 0 {
		t.Fatalf("stale binding reached capability probe and concurrent removal: calls=%d", calls)
	}
	if got := cliGit(t, gateDir, "rev-parse", "refs/heads/"+branch); got != plannedHead {
		t.Fatalf("refusal moved planned private head: got %s want %s", got, plannedHead)
	}
	plannedArchive := "refs/tags/no-mistakes-abandoned/" + branch + "/" + plannedHead
	if got := cliGit(t, gateDir, "for-each-ref", "--format=%(refname)", plannedArchive); got != "" {
		t.Fatalf("refusal archived planned private head: %s", got)
	}
	if _, err := os.Stat(postReceiveMarker); !os.IsNotExist(err) {
		t.Fatalf("refused stale binding reached push: %v", err)
	}
	if got := cliGit(t, work, "rev-parse", "HEAD"); got != candidate {
		t.Fatalf("candidate changed: got %s want %s", got, candidate)
	}
	if got := cliGit(t, work, "status", "--porcelain"); got != "" {
		t.Fatalf("worktree changed: %s", got)
	}
}

// Model the operator sequence, not just two divergent refs: publication,
// cancellation with an unpublished head, archive-backed keep-local recovery,
// then reconstruction on the published line. Recovery correctly leaves the
// admission ref at the kept head; it does not certify the later reconstruction.
func TestTriggerAfterArchiveRecovery(t *testing.T) {
	for _, tc := range []struct {
		name, mode, hook, wantError                string
		launchNonce                                string
		omitPrivate, fastForward, moveHead, active bool
		delayReceipt, retryAfterError, oldDaemon   bool
	}{
		{name: "ordinary rewritten", mode: "ordinary"},
		{name: "proof rewritten", mode: "proof"},
		{name: "proof ref-safe nonce", mode: "proof", launchNonce: "proof~1"},
		{name: "proof old daemon", mode: "proof", oldDaemon: true, wantError: "too old"},
		{name: "proof old daemon without reconciliation", mode: "proof", oldDaemon: true, fastForward: true},
		{name: "proof delayed registration", mode: "proof", delayReceipt: true},
		{name: "proof retry after registration error", mode: "proof", retryAfterError: true},
		{name: "proof fast forward", mode: "proof", fastForward: true},
		{name: "proof pins captured head", mode: "proof", moveHead: true},
		{name: "proof active pipeline ownership", mode: "proof", active: true, wantError: "pipeline"},
		{name: "proof absent private content", mode: "proof", omitPrivate: true, wantError: "at-risk"},
		{name: "proof rejected submission", mode: "proof", hook: "reject", wantError: "submission-rejected"},
		{name: "proof concurrent ref survives rejection", mode: "proof", hook: "concurrent", wantError: "submission-rejected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			dir := t.TempDir()
			p := paths.WithRoot(makeSocketSafeTempDir(t))
			t.Setenv("NM_HOME", p.Root())
			if err := p.EnsureDirs(); err != nil {
				t.Fatal(err)
			}
			d, err := db.Open(p.DB())
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			cliGit(t, dir, "init", "-b", "main")
			cliGit(t, dir, "config", "user.name", "Test")
			cliGit(t, dir, "config", "user.email", "test@example.com")
			cliGit(t, dir, "commit", "--allow-empty", "-m", "base")
			base := cliGit(t, dir, "rev-parse", "HEAD")
			cliGit(t, dir, "checkout", "-b", "feature/reconstructed")
			write := func(name, content string) string {
				t.Helper()
				if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
					t.Fatal(err)
				}
				cliGit(t, dir, "add", name)
				cliGit(t, dir, "commit", "-m", name)
				return cliGit(t, dir, "rev-parse", "HEAD")
			}
			privateHead := write("feature.txt", "feature\n")
			cliGit(t, dir, "branch", "archive/caller", privateHead)
			cliGit(t, dir, "reset", "--hard", base)
			write("advanced.txt", "advanced\n")
			published := cliGit(t, dir, "rev-parse", "HEAD")
			if !tc.omitPrivate {
				published = write("feature.txt", "feature\n")
			}
			preserved := write("feature.txt", "unpublished pipeline rewrite\n")
			cliGit(t, dir, "branch", "archive/pipeline", preserved)
			repo, err := d.InsertRepo(dir, "https://example.com/repo.git", "main")
			if err != nil {
				t.Fatal(err)
			}
			gateDir := p.RepoDir(repo.ID)
			cliGit(t, dir, "clone", "--bare", dir, gateDir)
			cliGit(t, gateDir, "config", "receive.advertisePushOptions", "true")
			cliGit(t, dir, "remote", "add", gate.RemoteName, gateDir)
			run, err := d.InsertRun(repo.ID, "feature/reconstructed", privateHead, base)
			if err != nil {
				t.Fatal(err)
			}
			if err := d.UpdateRunPushBinding(run.ID, db.PushBinding{HeadSHA: published, TargetKind: "upstream", TargetFingerprint: branchsync.TargetFingerprint(repo.PushURL()), Ref: "refs/heads/feature/reconstructed"}); err != nil {
				t.Fatal(err)
			}
			if err := d.UpdateRunStatusWithVerifiedHead(run.ID, types.RunCancelled, preserved); err != nil {
				t.Fatal(err)
			}
			recoveryRef := "refs/no-mistakes/recover/" + run.ID
			cliGit(t, dir, "push", gateDir, preserved+":"+recoveryRef)
			cliGit(t, dir, "reset", "--hard", privateHead)
			service := branchsync.Service{DB: d, Repo: repo, WorkDir: dir, GateDir: gateDir, Paths: p}
			if state := service.BindRecoveryArchive(ctx, "refs/heads/archive/pipeline"); state.Recovery == nil || state.Recovery.ArchiveRef != "refs/heads/archive/pipeline" {
				t.Fatalf("bind archive: %+v", state)
			}
			if state := service.Recover(ctx, true); !state.Recovered {
				t.Fatalf("recover: %+v", state)
			}
			if got := cliGit(t, gateDir, "rev-parse", "refs/heads/feature/reconstructed"); got != privateHead {
				t.Fatalf("recovery head = %s, want %s", got, privateHead)
			}
			if !tc.fastForward {
				cliGit(t, dir, "reset", "--hard", published)
			}
			candidate := write("candidate.txt", "new operator work\n")
			state := service.InspectCached(ctx)
			if !tc.fastForward && (state.State != branchsync.StateLocalAhead || state.NextAction == nil || state.NextAction.Code != "run_pipeline") {
				t.Fatalf("reconstructed guidance: %+v", state)
			}
			localHead := candidate
			if tc.moveHead {
				// Proof admission must certify the captured commit, not this
				// later HEAD, even though both contain the old private content.
				localHead = write("later.txt", "not part of the captured submission\n")
			}
			if tc.active {
				active, err := d.InsertRun(repo.ID, run.Branch, privateHead, base)
				if err != nil {
					t.Fatal(err)
				}
				if err := d.UpdateRunHeadSHA(active.ID, preserved); err != nil {
					t.Fatal(err)
				}
				if err := d.UpdateRunStatus(active.ID, types.RunRunning); err != nil {
					t.Fatal(err)
				}
			}
			optionsFile := filepath.ToSlash(filepath.Join(gateDir, "received-options"))
			postReceive := "#!/bin/sh\n: > '" + optionsFile + "'\ni=0\nwhile [ $i -lt \"$GIT_PUSH_OPTION_COUNT\" ]; do\n eval \"opt=\\$GIT_PUSH_OPTION_$i\"\n printf '%s\\n' \"$opt\" >> '" + optionsFile + "'\n i=$((i+1))\ndone\n"
			if err := os.WriteFile(filepath.Join(gateDir, "hooks", "post-receive"), []byte(postReceive), 0o755); err != nil {
				t.Fatal(err)
			}
			if tc.hook != "" {
				hook := "#!/bin/sh\n"
				if tc.hook == "concurrent" {
					hook += "unset GIT_QUARANTINE_PATH\ngit update-ref refs/heads/feature/reconstructed " + preserved + " " + strings.Repeat("0", 40) + " || exit 2\n"
				}
				hook += "echo submission-rejected >&2\nexit 1\n"
				if err := os.WriteFile(filepath.Join(gateDir, "hooks", "pre-receive"), []byte(hook), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			srv := ipc.NewServer()
			srv.Handle(ipc.MethodGetActiveRun, func(context.Context, json.RawMessage) (interface{}, error) { return &ipc.GetActiveRunResult{}, nil })
			srv.Handle(ipc.MethodGetRunsForHead, func(context.Context, json.RawMessage) (interface{}, error) {
				if _, err := os.Stat(optionsFile); err != nil {
					return &ipc.GetRunsResult{}, nil
				}
				return &ipc.GetRunsResult{Runs: []ipc.RunInfo{{ID: "new-run", HeadSHA: candidate}}}, nil
			})
			if !tc.oldDaemon {
				srv.Handle(ipc.MethodProbeProofReconciliation, func(context.Context, json.RawMessage) (interface{}, error) {
					return &ipc.ProbeProofReconciliationResult{OK: true}, nil
				})
			}
			claimCalls := 0
			srv.Handle(ipc.MethodClaimLaunchReceipt, func(_ context.Context, raw json.RawMessage) (interface{}, error) {
				var req ipc.ClaimLaunchReceiptParams
				if err := json.Unmarshal(raw, &req); err != nil {
					return nil, err
				}
				claimCalls++
				if tc.retryAfterError && claimCalls == 1 {
					return nil, fmt.Errorf("registration unavailable")
				}
				if tc.delayReceipt || tc.retryAfterError {
					return &ipc.ClaimLaunchReceiptResult{}, nil
				}
				return &ipc.ClaimLaunchReceiptResult{Receipt: &ipc.LaunchReceipt{RunID: "new-run", LaunchNonce: req.LaunchNonce, ValidationGeneration: req.ValidationGeneration, SubmittedHeadSHA: req.SubmittedHeadSHA, IntentDigest: req.IntentDigest}}, nil
			})
			srv.Handle(ipc.MethodStartFreshRun, func(_ context.Context, raw json.RawMessage) (interface{}, error) {
				var req ipc.StartFreshRunParams
				if err := json.Unmarshal(raw, &req); err != nil {
					return nil, err
				}
				if (tc.delayReceipt || tc.retryAfterError) && req.ReconciledPreviousHead != privateHead {
					t.Fatalf("fresh fallback lost previous-head provenance: %+v", req)
				}
				return &ipc.StartFreshRunResult{Receipt: ipc.LaunchReceipt{RunID: "new-run", LaunchNonce: req.LaunchNonce, ValidationGeneration: req.ValidationGeneration, SubmittedHeadSHA: req.HeadSHA, IntentDigest: digestLaunchIntent(req.Intent)}}, nil
			})
			done := make(chan error, 1)
			go func() { done <- srv.Serve(p.Socket()) }()
			t.Cleanup(func() { srv.Close(); <-done })
			var client *ipc.Client
			deadline := time.Now().Add(3 * time.Second)
			for {
				client, err = ipc.Dial(p.Socket())
				if err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal(err)
				}
				time.Sleep(10 * time.Millisecond)
			}
			defer client.Close()
			chdir(t, dir)
			env := &axiEnv{p: p, d: d, repo: repo, cfg: config.DefaultGlobalConfig(), client: client}
			var receipt *ipc.LaunchReceipt
			nonce := tc.launchNonce
			if nonce == "" {
				nonce = "nonce"
			}
			if tc.mode == "ordinary" {
				_, err = triggerRun(ctx, env, run.Branch, nil, "validate reconstructed work", "", false)
			} else if tc.retryAfterError {
				if _, firstErr := triggerProofRun(ctx, env, run.Branch, candidate, nil, "validate reconstructed work", "", false, nonce, "generation"); firstErr == nil || !strings.Contains(firstErr.Error(), "registration unavailable") {
					t.Fatalf("first proof attempt error = %v", firstErr)
				}
				receipt, err = triggerProofRun(ctx, env, run.Branch, candidate, nil, "validate reconstructed work", "", false, nonce, "generation")
			} else {
				receipt, err = triggerProofRun(ctx, env, run.Branch, candidate, nil, "validate reconstructed work", "", false, nonce, "generation")
			}
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) || receipt != nil {
					t.Fatalf("want %s refusal without receipt, got %+v, %v", tc.wantError, receipt, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			wantHead := candidate
			if tc.wantError != "" {
				wantHead = privateHead
			}
			if tc.hook == "concurrent" {
				wantHead = preserved
			}
			if got := cliGit(t, gateDir, "rev-parse", "refs/heads/"+run.Branch); got != wantHead {
				t.Fatalf("gate head = %s, want %s", got, wantHead)
			}
			if got := cliGit(t, dir, "rev-parse", "HEAD"); got != localHead {
				t.Fatalf("candidate changed: %s", got)
			}
			if got := cliGit(t, dir, "status", "--porcelain"); got != "" {
				t.Fatalf("worktree changed: %s", got)
			}
			if got := cliGit(t, gateDir, "rev-parse", recoveryRef); got != preserved {
				t.Fatalf("recovery evidence changed: %s", got)
			}
			if got := cliGit(t, dir, "rev-parse", "refs/heads/archive/pipeline"); got != preserved {
				t.Fatalf("archive changed: %s", got)
			}
			archive := "refs/tags/no-mistakes-abandoned/" + run.Branch + "/" + privateHead
			if !tc.oldDaemon && !tc.omitPrivate && !tc.fastForward && !tc.active {
				if got := cliGit(t, gateDir, "rev-parse", archive); got != privateHead {
					t.Fatalf("admission archive = %s", got)
				}
			} else if !tc.oldDaemon {
				if got := cliGit(t, gateDir, "for-each-ref", "--format=%(refname)", archive); got != "" {
					t.Fatalf("unexpected reconciliation archive: %s", got)
				}
			}
			oldRun, err := d.GetRun(run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if oldRun.CustodyReturnedAt == nil || oldRun.HeadSHA != preserved || oldRun.LastPushedSHA == nil || *oldRun.LastPushedSHA != published {
				t.Fatalf("recovered run provenance changed: %+v", oldRun)
			}
			if tc.wantError != "" {
				if _, err := os.Stat(optionsFile); !os.IsNotExist(err) {
					t.Fatalf("rejected submission reached post-receive: %v", err)
				}
			}
			if tc.wantError == "" {
				options, err := os.ReadFile(optionsFile)
				if err != nil {
					t.Fatal(err)
				}
				if !tc.fastForward && !strings.Contains(string(options), formatReconciledPreviousHeadPushOption(privateHead)) {
					t.Fatalf("missing previous-head provenance: %s", options)
				}
				if tc.mode == "proof" {
					if receipt.SubmittedHeadSHA != candidate || receipt.LaunchNonce != nonce || receipt.ValidationGeneration != "generation" {
						t.Fatalf("receipt lost identity: %+v", receipt)
					}
					for _, opt := range []string{formatLaunchNoncePushOption(nonce), formatValidationGenerationPushOption("generation")} {
						if !strings.Contains(string(options), opt) {
							t.Fatalf("missing option %s: %s", opt, options)
						}
					}
				}
			}
		})
	}
}
