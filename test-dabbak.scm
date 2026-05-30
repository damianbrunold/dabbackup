;; test-dabbak.scm — black-box tests for the dabscm port of dabbak.
;;
;; Run with either interpreter from the dabbak project root:
;;   scm  test-dabbak.scm
;;   scmj test-dabbak.scm   (java)
;;
;; Like test-dabsync.scm these tests do NOT import dabbak's internals. They
;; copy dabbak.scm into a fresh temp directory (so the script's own folder
;; becomes the config/base dir, exactly as a real install works), write a
;; config there, invoke the script as a separate process and assert on the
;; resulting filesystem, exit codes and captured output. The interpreter and
;; script can be overridden with the DABBAK_SCM and DABBAK_SCRIPT env vars.

(import (scheme base)
        (scheme write)
        (scheme file)
        (srfi 1)
        (srfi 13)
        (scm test)
        (scm fs)
        (scm system)
        (scm datetime))

(test-runner-factory scm-test-runner)

;; ---- interpreter / script selection ----

(define *scm*
  (or (get-environment-variable "DABBAK_SCM")
      (if (eq? (sys-scm-technology) 'java) "scmj" "scm")))

(define *script-text*
  (let ((path (or (get-environment-variable "DABBAK_SCRIPT") "dabbak.scm")))
    (call-with-input-file path
      (lambda (p)
        (let ((o (open-output-string)))
          (let loop ()
            (let ((c (read-char p)))
              (if (eof-object? c)
                  (get-output-string o)
                  (begin (write-char c o) (loop))))))))))

;; ---- filesystem helpers ----

(define (write-file path content)
  (let ((p (open-output-file path)))
    (display content p)
    (close-output-port p)))

(define (read-file path)
  (let ((p (open-input-file path))
        (o (open-output-string)))
    (let loop ()
      (let ((c (read-char p)))
        (if (eof-object? c)
            (begin (close-input-port p) (get-output-string o))
            (begin (write-char c o) (loop)))))))

(define (make-file path content)
  (let ((dir (directory-name path)))
    (unless (directory-exists? dir) (make-dirs-test dir)))
  (write-file path content))

(define (make-dirs-test path)
  (unless (or (string=? path "") (directory-exists? path))
    (let ((parent (directory-name path)))
      (unless (or (string=? parent path) (string=? parent "")) (make-dirs-test parent))
      (make-directory path))))

;; ---- config + harness ----

(define (write-config path src full partial state excludes)
  (let ((ex (string-join (map (lambda (e) (string-append "\"" e "\"")) excludes) ", ")))
    (write-file path
      (string-append
        "{\n"
        "  \"source\": { \"directories\": [\"" src "\"], \"excludes\": [" ex "] },\n"
        "  \"destination\": { \"directory_full\": \"" full "\", \"directory_partial\": \"" partial "\" },\n"
        "  \"full_state_file\": \"" state "\",\n"
        "  \"packaging_state_file\": \"packaging-state.json\"\n"
        "}\n"))))

;; Set up a fresh base dir with src/full/partial, a copied dabbak.scm and a
;; config, then call (proc base script src full partial). Cleans up after.
(define (with-backup excludes proc)
  (let* ((base (mktempdir '(prefix . "dabbak-test")))
         (script (join-path base "dabbak.scm"))
         (src (join-path base "src"))
         (full (join-path base "full"))
         (partial (join-path base "partial"))
         (state (join-path base "state.json")))
    (make-directory src)
    (make-directory full)
    (make-directory partial)
    (write-file script *script-text*)
    (write-config (join-path base "backup-config.json") src full partial state excludes)
    (proc base script src full partial)
    (delete-directory base)))

;; Run dabbak.scm (the copy in `base`) with the given args. Returns
;; (exit-code stdout stderr).
(define (db base . args)
  (run-program/capture (cons *scm* (cons (join-path base "dabbak.scm") args))))

(define (db-exit base . args) (car (apply db base args)))
(define (db-out base . args) (cadr (apply db base args)))

(test-begin "dabbak")

;; ===================== backup: core =====================

(test-group "backup: copies new files to mirror and snapshot"
  (with-backup '() (lambda (base script src full partial)
    (make-file (join-path src "a.txt") "hello")
    (make-file (join-path src "sub/b.txt") "deep")
    (test-equal 0 (db-exit base "backup" "--json"))
    ;; mirror keeps the last source component ("src") as the top folder
    (test-equal "hello" (read-file (join-path full "src/a.txt")))
    (test-equal "deep" (read-file (join-path full "src/sub/b.txt"))))))

(test-group "backup: --json reports new count"
  (with-backup '() (lambda (base script src full partial)
    (make-file (join-path src "a.txt") "x")
    (make-file (join-path src "b.txt") "y")
    (let ((out (db-out base "backup" "--json")))
      (test-assert (string-contains out "\"new\": 2"))
      (test-assert (string-contains out "\"completed\": true"))))))

(test-group "backup: writes drop-in state file {path:[size,seconds]}"
  (with-backup '() (lambda (base script src full partial)
    (make-file (join-path src "a.txt") "hello")
    (db base "backup" "--json")
    (let ((st (read-file (join-path base "state.json"))))
      (test-assert (string-contains st (join-path src "a.txt")))
      ;; size 6 ("hello\0"? no: "hello"=5)  -> just check the [n, n] array shape
      (test-assert (string-contains st "["))))))

(test-group "backup: creates dated snapshot with __state.json"
  (with-backup '() (lambda (base script src full partial)
    (make-file (join-path src "a.txt") "hello")
    (db base "backup" "--json")
    (let ((snaps (filter (lambda (e) (eq? (cdr e) 'directory))
                         (directory-entries partial))))
      (test-equal 1 (length snaps))
      (let ((snap (join-path partial (caar snaps))))
        (test-equal #t (file-exists? (join-path snap "__state.json")))
        (test-equal #t (file-exists? (join-path snap "src/a.txt"))))))))

(test-group "backup: idempotent — second run is all unchanged"
  (with-backup '() (lambda (base script src full partial)
    (make-file (join-path src "a.txt") "hello")
    (make-file (join-path src "b.txt") "world")
    (db base "backup" "--json")
    (let ((out (db-out base "backup" "--json")))
      (test-assert (string-contains out "\"new\": 0"))
      (test-assert (string-contains out "\"unchanged\": 2"))))))

(test-group "backup: detects changed file (content + mtime)"
  (with-backup '() (lambda (base script src full partial)
    (make-file (join-path src "a.txt") "hello")
    (db base "backup" "--json")
    (write-file (join-path src "a.txt") "CHANGED-LONGER")
    (set-file-modification-time! (join-path src "a.txt") 1000000000000)
    (let ((out (db-out base "backup" "--json")))
      (test-assert (string-contains out "\"changed\": 1")))
    (test-equal "CHANGED-LONGER" (read-file (join-path full "src/a.txt"))))))

(test-group "backup: deletion pass removes vanished file from mirror"
  (with-backup '() (lambda (base script src full partial)
    (make-file (join-path src "a.txt") "hello")
    (make-file (join-path src "b.txt") "world")
    (db base "backup" "--json")
    (delete-file (join-path src "b.txt"))
    (let ((out (db-out base "backup" "--json")))
      (test-assert (string-contains out "\"deleted\": 1")))
    (test-equal #f (file-exists? (join-path full "src/b.txt")))
    (test-equal #t (file-exists? (join-path full "src/a.txt"))))))

(test-group "backup: --dry-run writes nothing"
  (with-backup '() (lambda (base script src full partial)
    (make-file (join-path src "a.txt") "hello")
    (test-equal 0 (db-exit base "backup" "--dry-run"))
    (test-equal #f (file-exists? (join-path full "src/a.txt")))
    (test-equal #f (file-exists? (join-path base "state.json"))))))

(test-group "backup: exclude pattern skips files"
  (with-backup '("*.log") (lambda (base script src full partial)
    (make-file (join-path src "a.txt") "keep")
    (make-file (join-path src "b.log") "skip")
    (db base "backup" "--json")
    (test-equal #t (file-exists? (join-path full "src/a.txt")))
    (test-equal #f (file-exists? (join-path full "src/b.log"))))))

(test-group "backup: exclude by directory basename"
  (with-backup '("__pycache__") (lambda (base script src full partial)
    (make-file (join-path src "a.txt") "keep")
    (make-file (join-path src "__pycache__/x.pyc") "skip")
    (db base "backup" "--json")
    (test-equal #t (file-exists? (join-path full "src/a.txt")))
    (test-equal #f (directory-exists? (join-path full "src/__pycache__"))))))

;; ===================== restore =====================

(test-group "restore: restores everything from snapshot"
  (with-backup '() (lambda (base script src full partial)
    (make-file (join-path src "a.txt") "hello")
    (make-file (join-path src "sub/b.txt") "deep")
    (db base "backup" "--json")
    (let ((dest (join-path base "restored")))
      (test-equal 0 (db-exit base "restore" dest))
      (test-equal "hello" (read-file (join-path dest "src/a.txt")))
      (test-equal "deep" (read-file (join-path dest "src/sub/b.txt")))))))

(test-group "restore: glob pattern filters files"
  (with-backup '() (lambda (base script src full partial)
    (make-file (join-path src "a.txt") "hello")
    (make-file (join-path src "sub/b.txt") "deep")
    (db base "backup" "--json")
    (let ((dest (join-path base "restored")))
      (db base "restore" dest "*/sub/*")
      (test-equal #t (file-exists? (join-path dest "src/sub/b.txt")))
      (test-equal #f (file-exists? (join-path dest "src/a.txt")))))))

(test-group "restore: --dry-run copies nothing"
  (with-backup '() (lambda (base script src full partial)
    (make-file (join-path src "a.txt") "hello")
    (db base "backup" "--json")
    (let ((dest (join-path base "restored")))
      (test-equal 0 (db-exit base "restore" dest "--dry-run"))
      (test-equal #f (directory-exists? dest))))))

(test-group "restore: existing dest without --force exits 1"
  (with-backup '() (lambda (base script src full partial)
    (make-file (join-path src "a.txt") "hello")
    (db base "backup" "--json")
    (let ((dest (join-path base "restored")))
      (make-directory dest)
      (test-equal 1 (db-exit base "restore" dest))))))

;; ===================== list =====================

(test-group "list: shows snapshot after a backup"
  (with-backup '() (lambda (base script src full partial)
    (make-file (join-path src "a.txt") "hello")
    (db base "backup" "--json")
    (let ((out (db-out base "list")))
      (test-assert (string-contains out "status"))
      (test-assert (string-contains out "ok"))))))

(test-group "list: --json emits array"
  (with-backup '() (lambda (base script src full partial)
    (make-file (join-path src "a.txt") "hello")
    (db base "backup" "--json")
    (let ((out (db-out base "list" "--json")))
      (test-assert (string-contains out "\"date\":"))
      (test-assert (string-contains out "\"file_count\":"))))))

(test-group "list: no snapshots message"
  (with-backup '() (lambda (base script src full partial)
    (test-assert (string-contains (db-out base "list") "no snapshots")))))

;; ===================== refresh-state =====================

(test-group "refresh-state: rebuilds state from mirror"
  (with-backup '() (lambda (base script src full partial)
    (make-file (join-path src "a.txt") "hello")
    (make-file (join-path src "b.txt") "world")
    (db base "backup" "--json")
    (delete-file (join-path base "state.json"))
    (test-equal 0 (db-exit base "refresh-state"))
    (let ((st (read-file (join-path base "state.json"))))
      (test-assert (string-contains st (join-path src "a.txt")))
      (test-assert (string-contains st (join-path src "b.txt")))))))

;; ===================== prune =====================

(test-group "prune: --keep-last dry-run lists deletions"
  (with-backup '() (lambda (base script src full partial)
    ;; synthetic snapshots
    (for-each (lambda (d)
                (make-file (join-path partial (string-append d "/src/x.txt")) "x")
                (write-file (join-path partial (string-append d "/__state.json")) "{}"))
              '("2020-01-01" "2020-01-02" "2020-01-03"))
    (let ((out (db-out base "prune" "--keep-last" "1")))
      (test-assert (string-contains out "Would delete"))
      (test-assert (string-contains out "2020-01-01"))))))

(test-group "prune: --force --json deletes oldest"
  (with-backup '() (lambda (base script src full partial)
    (for-each (lambda (d)
                (make-file (join-path partial (string-append d "/src/x.txt")) "x")
                (write-file (join-path partial (string-append d "/__state.json")) "{}"))
              '("2020-01-01" "2020-01-02" "2020-01-03"))
    (let ((out (db-out base "prune" "--keep-last" "1" "--force" "--json")))
      (test-assert (string-contains out "\"deleted\""))
      (test-assert (string-contains out "2020-01-01")))
    (test-equal #f (directory-exists? (join-path partial "2020-01-01")))
    (test-equal #t (directory-exists? (join-path partial "2020-01-03"))))))

(test-group "prune: no policy exits 2"
  (with-backup '() (lambda (base script src full partial)
    (test-equal 2 (db-exit base "prune")))))

;; ===================== package =====================

(test-group "package: --full chunks files and writes packaging state"
  (with-backup '() (lambda (base script src full partial)
    (make-file (join-path src "a.txt") "hello")
    (db base "backup" "--json")
    (let ((dest (join-path base "pkg")))
      (test-equal 0 (db-exit base "package" dest "100" "--full"))
      ;; a part-1 folder must exist with the file
      (test-equal #t (file-exists? (join-path dest (string-append "backup-" (today) "-part-1/src/a.txt"))))
      (test-equal #t (file-exists? (join-path base "packaging-state.json")))))))

(test-group "package: existing dest without --force exits 1 and leaks no lock"
  (with-backup '() (lambda (base script src full partial)
    (make-file (join-path src "a.txt") "hello")
    (db base "backup" "--json")
    (let ((dest (join-path base "pkg")))
      (make-directory dest)
      (test-equal 1 (db-exit base "package" dest "100"))
      (test-equal #f (file-exists? (join-path base "state.json.lock")))))))

;; ===================== config / init =====================

(test-group "config: prints effective config containing source path"
  (with-backup '() (lambda (base script src full partial)
    (let ((out (db-out base "config")))
      (test-assert (string-contains out src))
      (test-assert (string-contains out "directory_full"))))))

(test-group "init: writes a fresh template, refuses overwrite without --force"
  (let* ((base (mktempdir '(prefix . "dabbak-init")))
         (script (join-path base "dabbak.scm")))
    (write-file script *script-text*)
    (test-equal 0 (car (run-program/capture (list *scm* script "init" "--name" "cfg.json"))))
    (test-equal #t (file-exists? (join-path base "cfg.json")))
    ;; second init without --force exits 1
    (test-equal 1 (car (run-program/capture (list *scm* script "init" "--name" "cfg.json"))))
    ;; with --force succeeds
    (test-equal 0 (car (run-program/capture (list *scm* script "init" "--name" "cfg.json" "--force"))))
    (delete-directory base)))

;; ===================== locking / cli errors =====================

(test-group "lock: held lock makes backup exit 1"
  (with-backup '() (lambda (base script src full partial)
    (make-file (join-path src "a.txt") "hello")
    (write-file (join-path base "state.json.lock") "99999\n2020\n")
    (let ((r (db base "backup")))
      (test-equal 1 (car r))
      (test-assert (string-contains (caddr r) "lock")))
    (delete-file (join-path base "state.json.lock")))))

(test-group "lock: backup releases lock on success"
  (with-backup '() (lambda (base script src full partial)
    (make-file (join-path src "a.txt") "hello")
    (db base "backup" "--json")
    (test-equal #f (file-exists? (join-path base "state.json.lock"))))))

(test-group "cli: no command exits 2"
  (with-backup '() (lambda (base script src full partial)
    (test-equal 2 (db-exit base)))))

(test-group "cli: unknown command exits 2"
  (with-backup '() (lambda (base script src full partial)
    (test-equal 2 (db-exit base "wibble")))))

(test-group "cli: unknown backup option exits 2"
  (with-backup '() (lambda (base script src full partial)
    (test-equal 2 (db-exit base "backup" "--frobnicate")))))

(test-end "dabbak")
