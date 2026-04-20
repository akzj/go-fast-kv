package internal

import (
    "fmt"
    "os"
    "path/filepath"
    "testing"

    kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

func TestBackupDebug6(t *testing.T) {
    storeDir := t.TempDir()
    s, err := Open(kvstoreapi.Config{Dir: storeDir})
    if err != nil {
        t.Fatalf("Open failed: %v", err)
    }

    for i := 0; i < 50; i++ {
        s.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
    }

    backupDir := filepath.Join(t.TempDir(), "backup")
    internalStore := s.(*store)
    err = internalStore.Backup(backupDir)
    t.Logf("Backup error: %v", err)

    // Check all critical files
    t.Log("=== Critical files comparison ===")
    
    // Check checkpoint
    origCP, _ := os.Stat(filepath.Join(storeDir, "checkpoint"))
    bakCP, _ := os.Stat(filepath.Join(backupDir, "checkpoint"))
    t.Logf("checkpoint: orig=%v bak=%v", origCP, bakCP)
    
    // Check page_segments
    origPS, _ := os.ReadDir(filepath.Join(storeDir, "page_segments"))
    bakPS, _ := os.ReadDir(filepath.Join(backupDir, "page_segments"))
    t.Logf("page_segments: orig=%d bak=%d", len(origPS), len(bakPS))
    if len(origPS) > 0 {
        origFi, _ := origPS[0].Info()
        bakFi, _ := bakPS[0].Info()
        t.Logf("  orig: %s (size=%d)", origPS[0].Name(), origFi.Size())
        t.Logf("  bak:  %s (size=%d)", bakPS[0].Name(), bakFi.Size())
    }
    
    // Check LSM
    origLSM, _ := os.ReadDir(filepath.Join(storeDir, "lsm"))
    bakLSM, _ := os.ReadDir(filepath.Join(backupDir, "lsm"))
    t.Logf("lsm: orig=%d bak=%d", len(origLSM), len(bakLSM))
    for i, e := range origLSM {
        fi, _ := e.Info()
        t.Logf("  orig[%d]: %s (size=%d)", i, e.Name(), fi.Size())
    }
    for i, e := range bakLSM {
        fi, _ := e.Info()
        t.Logf("  bak[%d]:  %s (size=%d)", i, e.Name(), fi.Size())
    }

    s.Close()

    restoreDir := filepath.Join(t.TempDir(), "restore")
    restored, err := Restore(backupDir, restoreDir)
    if err != nil {
        t.Fatalf("Restore failed: %v", err)
    }
    
    // Check restore dir LSM
    restoreLSM, _ := os.ReadDir(filepath.Join(restoreDir, "lsm"))
    t.Logf("restore lsm: %d entries", len(restoreLSM))
    for i, e := range restoreLSM {
        fi, _ := e.Info()
        t.Logf("  restore[%d]: %s (size=%d)", i, e.Name(), fi.Size())
    }

    val, err := restored.Get([]byte("k0"))
    if err != nil {
        t.Errorf("Get k0 failed: %v", err)
    } else {
        t.Logf("Got k0: %s", string(val))
    }
    restored.Close()
}
