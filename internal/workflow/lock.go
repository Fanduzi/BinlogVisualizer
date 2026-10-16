package workflow

import "binlogviz/internal/clierr"

// LockFile is the per-root lock taken by run, resume and clean.
const LockFile = ".lock"

// ReasonLocked is details.reason when another workflow command holds the lock.
const ReasonLocked = "locked"

func lockedErr(path string) error {
	return clierr.Reasonf(clierr.KindRefused, ReasonLocked,
		"another binlogviz workflow command (run, resume or clean) is using this directory (lock %s held); wait for it to finish", path)
}
