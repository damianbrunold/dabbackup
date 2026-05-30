;; dabbak.scm — incremental backup tool with dated partial snapshots.
;;
;; A Scheme (dabscm) port of dabbak.py. Maintains one always-current "full"
;; mirror plus dated "partial" snapshots of incremental changes, allowing
;; point-in-time restore. Single self-contained file; runs under `scm`
;; (C#) or `scmj` (Java). Long paths on Windows are handled inside the
;; (scm fs) primitives.
;;
;; Usage:
;;   scm dabbak.scm <command> [options]
;; Commands:
;;   init | backup | restore | list | prune | package | refresh-state | config
;;
;; Drop-in compatible with the Python version: same config file, same
;; JSON state format ({path: [size, seconds]}), same snapshot layout
;; (dated dirs + __state.json + __incomplete marker), same CLI surface.
;;
;; JSON read/write/pretty-print comes from the (scm json simple) library and
;; the per-config lock uses the (scm fs) file-lock primitive — a kernel
;; advisory lock that auto-releases on process death (no stale locks). There
;; is no GUI (the dabscm runtime has no windowing toolkit); use legacy-python
;; for the Tkinter GUI.

(import (scheme base)
        (scheme write)
        (scheme file)
        (scheme process-context)
        (srfi 1)
        (srfi 13)
        (srfi 69)
        (srfi 132)
        (scm fs)
        (scm system)
        (scm string)
        (scm datetime)
        (scm json simple))

;; ======================================================================
;; Small utilities
;; ======================================================================

(define *windows?* (eq? (sys-platform) 'windows))
(define *sep* (if *windows?* "\\" "/"))
(define *sepchar* (string-ref *sep* 0))

(define (eprint . args)
  (for-each (lambda (a) (display a (current-error-port))) args)
  (newline (current-error-port)))

(define (prn . args)
  (for-each display args)
  (newline))

(define (->string x)
  (if (string? x) x
      (let ((p (open-output-string))) (display x p) (get-output-string p))))

(define (err->string e)
  (if (error-object? e) (error-object-message e) (->string e)))

;; python normpath: collapse, then strip a single trailing separator.
(define (norm-path p)
  (let* ((n (normalized-path p)) (len (string-length n)))
    (if (and (> len 1) (char=? (string-ref n (- len 1)) *sepchar*))
        (substring n 0 (- len 1))
        n)))

(define (rstrip-char s c)
  (let loop ((len (string-length s)))
    (if (and (> len 0) (char=? (string-ref s (- len 1)) c))
        (loop (- len 1))
        (substring s 0 len))))

(define (read-file->string path)
  (call-with-input-file path
    (lambda (p)
      (let ((o (open-output-string)))
        (let loop ()
          (let ((c (read-char p)))
            (if (eof-object? c)
                (get-output-string o)
                (begin (write-char c o) (loop)))))))))

(define (read-json-file path) (json-parse (read-file->string path)))

;; directory listing tolerant of a missing path (returns '()).
(define (entries-safe path)
  (if (directory-exists? path) (directory-entries path) '()))

(define (sorted-entries path)
  (list-sort (lambda (a b) (string<? (car a) (car b))) (entries-safe path)))

(define (names-sorted-desc path)
  (list-sort (lambda (a b) (string>? a b)) (map car (entries-safe path))))

;; one-decimal formatting for human-readable sizes.
(define (one-decimal x)
  (let* ((scaled (exact (round (* x 10))))
         (whole (quotient scaled 10))
         (frac (abs (remainder scaled 10))))
    (string-append (number->string whole) "." (number->string frac))))

(define (format-size n)
  (let loop ((n n) (units '("B" "KB" "MB" "GB" "TB")))
    (let ((unit (car units)))
      (if (or (< n 1024) (string=? unit "TB"))
          (if (string=? unit "B")
              (string-append (number->string (exact (floor n))) " B")
              (string-append (one-decimal n) " " unit))
          (loop (/ n 1024) (cdr units))))))

;; insert thousands separators into a non-negative integer.
(define (group-digits n)
  (let* ((s (number->string n)) (len (string-length s)))
    (let loop ((i 0) (out '()))
      (if (>= i len)
          (list->string (reverse out))
          (let ((rem (- len i)))
            (loop (+ i 1)
                  (cons (string-ref s i)
                        (if (and (> i 0) (= (modulo rem 3) 0)) (cons #\, out) out))))))))

(define (valid-date-string? s)
  (and (string? s)
       (string-matches s "^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$")
       #t))

(define (today-str) (today))         ;; "YYYY-MM-DD" (local)
(define (now-str) (now 'iso))         ;; "YYYY-MM-DD HH:MM" (local)

;; ======================================================================
;; fnmatch (gitignore-flavored), implemented via fnmatch->regex.
;; fnmatch '*' matches path separators (unlike a typical glob), matching
;; python's fnmatch used by dabbak's exclude/pattern logic.
;; ======================================================================

(define (regex-special? c)
  (memv c '(#\. #\^ #\$ #\+ #\{ #\} #\( #\) #\| #\\ #\])))

(define (fnmatch->regex pat)
  (let ((out (open-output-string)) (len (string-length pat)))
    (write-char #\^ out)
    (let loop ((i 0))
      (if (>= i len)
          (begin (write-char #\$ out) (get-output-string out))
          (let ((c (string-ref pat i)))
            (cond
              ((char=? c #\*) (write-string ".*" out) (loop (+ i 1)))
              ((char=? c #\?) (write-char #\. out) (loop (+ i 1)))
              ((char=? c #\[)
               (let ((j (class-end pat (+ i 1) len)))
                 (if (not j)
                     (begin (write-string "\\[" out) (loop (+ i 1)))
                     (begin
                       (write-char #\[ out)
                       (let* ((neg (and (< (+ i 1) len) (char=? (string-ref pat (+ i 1)) #\!)))
                              (k (if neg (+ i 2) (+ i 1))))
                         (when neg (write-char #\^ out))
                         (let cl ((m k))
                           (when (< m j)
                             (let ((cc (string-ref pat m)))
                               (when (char=? cc #\\) (write-char #\\ out))
                               (write-char cc out))
                             (cl (+ m 1)))))
                       (write-char #\] out)
                       (loop (+ j 1))))))
              ((regex-special? c) (write-char #\\ out) (write-char c out) (loop (+ i 1)))
              (else (write-char c out) (loop (+ i 1)))))))))

;; find the index of the ']' closing a class started at `start` (the char
;; after '['). A ']' as the first class char is literal. #f if unterminated.
(define (class-end pat start len)
  (let ((first (if (and (< start len) (char=? (string-ref pat start) #\!)) (+ start 1) start)))
    (let loop ((i (if (and (< first len) (char=? (string-ref pat first) #\])) (+ first 1) first)))
      (cond
        ((>= i len) #f)
        ((char=? (string-ref pat i) #\]) i)
        (else (loop (+ i 1)))))))

(define (fnmatch? name pat)
  (let ((nm (if *windows?* (string-downcase name) name))
        (pt (if *windows?* (string-downcase pat) pat)))
    (and (string-matches nm (fnmatch->regex pt)) #t)))

(define (has-glob? s)
  (or (string-index s #\*) (string-index s #\?) (string-index s #\[)))

(define (has-slash? s)
  (or (string-index s #\/) (string-index s #\\)))

;; compile a list of exclude entries into a (path -> boolean) predicate.
(define (compile-excludes excludes)
  (let loop ((es excludes) (basenames '()) (fullpaths '()) (abspaths '()))
    (if (null? es)
        (let ((basenames (reverse basenames)) (fullpaths (reverse fullpaths)))
          (lambda (path)
            (or (any (lambda (a) (or (string=? a path) (string=? a (norm-path path)))) abspaths)
                (let ((base (base-name path)))
                  (any (lambda (p) (fnmatch? base p)) basenames))
                (any (lambda (p) (fnmatch? path p)) fullpaths))))
        (let ((raw (car es)))
          (cond
            ((not (has-slash? raw)) (loop (cdr es) (cons raw basenames) fullpaths abspaths))
            ((has-glob? raw)        (loop (cdr es) basenames (cons raw fullpaths) abspaths))
            (else                   (loop (cdr es) basenames fullpaths (cons (norm-path raw) abspaths))))))))

;; ======================================================================
;; walk — recursive file traversal, sorted, skipping symlinks/excludes.
;; calls (proc filepath) for each file (mirrors the python generator).
;; ======================================================================

(define (walk-files directory excluded? proc)
  (cond
    ((file-exists? directory)
     (unless (excluded? directory) (proc directory)))
    ((excluded? directory) #f)
    ((directory-exists? directory)
     (for-each
       (lambda (entry)
         (let ((full (join-path directory (car entry)))
               (type (cdr entry)))
           (cond
             ((excluded? full) #f)
             ((eq? type 'symlink) #f)          ;; skip symlinks (and junctions)
             ((eq? type 'directory) (walk-files full excluded? proc))
             ((eq? type 'file) (proc full)))))
       (sorted-entries directory)))
    (else #f)))

;; ======================================================================
;; config / state
;; ======================================================================

(define *script-path* (car (command-line)))
(define (base-dir) (directory-name (absolute-path *script-path*)))

(define (aget obj key) (json-ref obj key))   ;; object lookup via (scm json simple)

(define (config-source-directories c)
  (let ((d (aget (aget c "source") "directories")))
    (if (vector? d) (vector->list d) '())))
(define (config-source-excludes c)
  (let ((e (aget (aget c "source") "excludes")))
    (if (vector? e) (vector->list e) '())))
(define (config-is-windows c)
  (let ((w (aget (aget c "source") "is-windows"))) (and (boolean? w) w)))
(define (config-dest-full c) (aget (aget c "destination") "directory_full"))
(define (config-dest-partial c) (aget (aget c "destination") "directory_partial"))
(define (config-full-state-file c) (aget c "full_state_file"))
(define (config-packaging-state-file c) (aget c "packaging_state_file"))

(define (read-config)
  (let* ((cfgfile (or (get-environment-variable "DABBAK_CONFIG") "backup-config.json"))
         (filepath (join-path (base-dir) cfgfile)))
    (if (path-exists? filepath)
        (read-json-file filepath)
        (begin (eprint "ERR: config not found: " filepath) (exit 1)))))

;; state is an alist ((path . #(size seconds)) ...). Build a hash-table for
;; O(1) lookups during the walk.
(define (read-full-state-file filepath)
  (if (path-exists? filepath) (read-json-file filepath) '()))

(define (state-alist->table alist)
  (let ((h (make-hash-table)))
    (for-each (lambda (pr) (hash-table-set! h (car pr) (cdr pr))) alist)
    h))

(define (read-full-state-table config)
  (state-alist->table (read-full-state-file (config-full-state-file config))))

;; write a state hash-table atomically (tmp + rename), keys sorted for a
;; stable, diff-friendly file. The python reader accepts any key order.
(define (write-state-table filepath table)
  (let* ((keys (list-sort string<? (hash-table-keys table)))
         (alist (map (lambda (k) (cons k (hash-table-ref table k))) keys))
         (tmp (string-append filepath ".tmp")))
    (call-with-output-file tmp (lambda (p) (json-write-pretty alist p)))
    (move-file tmp filepath)))

;; ======================================================================
;; path prefix logic (mirror of compute_prefixlen / find_source_prefix /
;; expand_source_dirs)
;; ======================================================================

(define (compute-prefixlen prefix)
  (let ((len (string-length prefix)))
    (if (and (> len 0)
             (let ((last (string-ref prefix (- len 1))))
               (or (char=? last #\\) (char=? last #\/))))
        len
        (+ len 1))))

(define (find-source-prefix config fullpath)
  (let* ((is-win-state (and (config-is-windows config) (string=? *sep* "/")))
         (sep (if is-win-state "\\" *sep*))
         (sepc (string-ref sep 0)))
    (let loop ((dirs (config-source-directories config)))
      (if (null? dirs)
          #f
          (let* ((source-dir (car dirs))
                 (is-wild (string-suffix? "*" source-dir))
                 (md0 (if is-wild (substring source-dir 0 (- (string-length source-dir) 1)) source-dir))
                 (match-dir (rstrip-char md0 sepc)))
            (if (and (not (string=? fullpath match-dir))
                     (not (string-prefix? (string-append match-dir sep) fullpath)))
                (loop (cdr dirs))
                (if is-wild
                    match-dir
                    (let ((idx (string-index-right match-dir sepc)))
                      (if idx (substring match-dir 0 idx) match-dir)))))))))

(define (expand-source-dirs directories)
  (append-map
    (lambda (srcdir0)
      (let ((srcdir (norm-path srcdir0)))
        (if (or (string-suffix? "*" srcdir)
                (string-suffix? (string-append "*" *sep*) srcdir))
            (let ((base (rstrip-char (rstrip-char srcdir #\*) *sepchar*)))
              (if (directory-exists? base)
                  (filter-map
                    (lambda (entry)
                      (and (eq? (cdr entry) 'directory) (join-path base (car entry))))
                    (sorted-entries base))
                  '()))
            (list srcdir))))
    directories))

;; ======================================================================
;; locking — kernel-managed advisory lock at <full_state_file>.lock via the
;; (scm fs) file-lock primitive. Unlike an existence lock it auto-releases on
;; process death (the OS frees it when the holder exits), so it never goes
;; stale; the leftover empty .lock file is harmless and is not the signal.
;; ======================================================================

(define (lock-path-for config) (string-append (config-full-state-file config) ".lock"))

;; run thunk under the per-config write lock. Exits 1 if another process holds
;; it. The OS releases the lock on process exit, so even an (exit) inside thunk
;; cannot leave it stale; file-unlock is still called on the normal/error path.
(define (with-lock config thunk)
  (let* ((lp (lock-path-for config))
         (handle (file-lock lp)))
    (if handle
        (let ((result (guard (e (#t (file-unlock handle) (raise e))) (thunk))))
          (file-unlock handle)
          result)
        (begin
          (eprint "ERROR: another dabbak run holds the lock at " lp)
          (exit 1)))))

;; ======================================================================
;; full-log rotation (10 MB, one rotated file kept)
;; ======================================================================

(define FULL-LOG-MAX-BYTES (* 10 1024 1024))

(define (get-full-log) (join-path (base-dir) "backup-full.log"))
(define (get-partial-log dest-partial-base today)
  (join-path dest-partial-base (string-append "backup-partial-" today ".log")))

(define (rotate-log-if-large path)
  (guard (e (#t #f))
    (when (and (path-exists? path) (>= (file-size path) FULL-LOG-MAX-BYTES))
      (let ((rotated (string-append path ".1")))
        (when (path-exists? rotated) (delete-file rotated))
        (move-file path rotated)))))

;; ======================================================================
;; remove a file and prune now-empty ancestor dirs up to (not past) stop.
;; returns #t if the file was removed, #f otherwise. (scm fs)'s
;; delete-directory is recursive, so we only call it on an empty dir.
;; ======================================================================

(define (remove-file-pruning filepath stop-dir)
  (guard (e (#t #f))
    (delete-file filepath)
    (let loop ((dirpath (directory-name filepath)))
      (when (and (string-prefix? stop-dir dirpath) (not (string=? dirpath stop-dir)))
        (guard (e2 (#t #f))
          (when (null? (entries-safe dirpath))
            (delete-directory dirpath)
            (loop (directory-name dirpath))))))
    #t))

;; ======================================================================
;; backup engine
;; ======================================================================

(define (mtime-changed? a b) (>= (abs (- a b)) 2))

(define (make-backup config dry-run quiet json-out)
  (let* ((today (today-str))
         (start-ms (timestamp))
         (source-dirs (expand-source-dirs (config-source-directories config)))
         (source-excludes (config-source-excludes config))
         (is-excluded? (compile-excludes source-excludes))
         (dest-full (norm-path (config-dest-full config)))
         (dest-partial-base (norm-path (config-dest-partial config)))
         (dest-partial (norm-path (join-path dest-partial-base today)))
         (state (read-full-state-table config))
         (new-state (make-hash-table))
         (full-log-port #f)
         (partial-log-port #f)
         (errors-full '())
         (errors-partial '())
         (stats (make-hash-table))
         (completed #f)
         (pg-enabled (not json-out))
         (pg-files 0) (pg-bytes 0) (pg-last 0)
         (pg-total (hash-table-size state)))

    (define (sget k) (hash-table-ref/default stats k 0))
    (define (sset! k v) (hash-table-set! stats k v))
    (define (sinc! k) (sset! k (+ (sget k) 1)))
    (define (sadd! k n) (sset! k (+ (sget k) n)))

    ;; plog: log files always; stdout gated by quiet/json-out.
    (define (plog msg . rest)
      (let ((dest (if (pair? rest) (car rest) 'both))
            (level (if (and (pair? rest) (pair? (cdr rest))) (cadr rest) 'info)))
        (when (memq dest '(full both))
          (display msg full-log-port) (newline full-log-port))
        (when (memq dest '(partial both))
          (display msg partial-log-port) (newline partial-log-port))
        (unless (or json-out (and quiet (memq level '(info file))))
          (prn msg))))

    (define (add-error! tag msg)
      (if (eq? tag 'partial)
          (set! errors-partial (cons msg errors-partial))
          (set! errors-full (cons msg errors-full))))

    (define (progress-tick! path size)
      (when pg-enabled
        (set! pg-files (+ pg-files 1))
        (set! pg-bytes (+ pg-bytes size))
        (let ((t (timestamp)))
          (when (>= (- t pg-last) 1000)
            (set! pg-last t)
            (eprint "[" pg-files " files, " (format-size pg-bytes) "] " path)))))

    (define (copy-into filepath destbase relpath overwrite tag)
      (let ((destpath (norm-path (join-path destbase relpath))))
        (if dry-run
            #t
            (guard (e (#t (let ((err (string-append "ERR: failed to copy " filepath " => " destpath)))
                            (add-error! tag err)
                            (plog err tag 'warn)
                            (plog (err->string e) tag 'warn))
                          #f))
              (make-directory (directory-name destpath))
              (when (and overwrite (path-exists? destpath)) (delete-file destpath))
              (if (eq? (copy-file filepath destpath) #f)
                  (let ((err (string-append "ERR: failed to copy " filepath " => " destpath)))
                    (add-error! tag err)
                    (plog err tag 'warn)
                    #f)
                  #t)))))

    (for-each (lambda (k) (sset! k 0)) '(new changed deleted unchanged failed bytes-copied))

    (rotate-log-if-large (get-full-log))
    (when (and dry-run (not json-out))
      (prn "DRY RUN: no files will be copied, deleted, or state written"))

    (unless (path-exists? dest-partial-base) (make-directory dest-partial-base))

    (set! full-log-port (open-output-file (get-full-log) 'append))
    (set! partial-log-port (open-output-file (get-partial-log dest-partial-base today) 'append))

    (plog (string-append "backup run " (now-str)))
    (plog "sources:")
    (for-each (lambda (s) (plog s)) source-dirs)
    (when (pair? source-excludes)
      (plog "excludes:")
      (for-each (lambda (e) (plog e)) source-excludes))
    (plog "destination:")
    (plog dest-full 'full)
    (plog dest-partial 'partial)
    (plog "read state")

    (unless (path-exists? dest-partial)
      (plog (string-append "create " dest-partial) 'partial)
      (make-directory dest-partial))

    ;; (sourcedir . prefixlen) for each expanded source dir
    (let ((source-prefixes
           (map (lambda (sd) (cons sd (compute-prefixlen (directory-name sd)))) source-dirs)))

      (define (relpath-for filepath)
        (let loop ((sps source-prefixes))
          (if (null? sps)
              #f
              (let ((sd (caar sps)) (plen (cdar sps)))
                (if (or (string=? filepath sd)
                        (string-prefix? (string-append sd *sep*) filepath))
                    (substring filepath plen (string-length filepath))
                    (loop (cdr sps)))))))

      (define (process-file filepath prefixlen)
        (let ((fsize (guard (e (#t #f)) (file-size filepath))))
          (if (not fsize)
              (let ((err (string-append "ERR: file " filepath " not found (fstat)")))
                (add-error! 'full err) (add-error! 'partial err)
                (plog err 'both 'warn))
              (let* ((fsec (quotient (file-modification-timestamp filepath) 1000))
                     (relpath (substring filepath prefixlen (string-length filepath)))
                     (entry (hash-table-ref/default state filepath #f)))
                (progress-tick! filepath fsize)
                (if entry
                    (let ((orig-size (vector-ref entry 0)) (orig-mtime (vector-ref entry 1)))
                      (if (or (not (= fsize orig-size)) (mtime-changed? fsec orig-mtime))
                          (begin
                            (plog (string-append "** " filepath) 'both 'file)
                            (let ((ok-p (copy-into filepath dest-partial relpath #t 'partial))
                                  (ok-f (copy-into filepath dest-full relpath #t 'full)))
                              (if (and ok-p ok-f)
                                  (begin (sinc! 'changed) (sadd! 'bytes-copied fsize)
                                         (hash-table-set! new-state filepath (vector fsize fsec)))
                                  (begin (hash-table-set! new-state filepath (vector orig-size orig-mtime))
                                         (sinc! 'failed)))))
                          (begin (sinc! 'unchanged)
                                 (hash-table-set! new-state filepath (vector fsize fsec)))))
                    (begin
                      (plog (string-append "++ " filepath) 'both 'file)
                      (let ((ok-p (copy-into filepath dest-partial relpath #f 'partial))
                            (ok-f (copy-into filepath dest-full relpath #f 'full)))
                        (if (and ok-p ok-f)
                            (begin (sinc! 'new) (sadd! 'bytes-copied fsize)
                                   (hash-table-set! new-state filepath (vector fsize fsec)))
                            (sinc! 'failed)))))))))

      ;; walk pass — guard catches any error (incl. interrupt) and leaves
      ;; completed=#f so the deletion pass is skipped and state is merged.
      (guard (e (#t (plog (string-append "interrupted by exception: " (err->string e)) 'both 'warn)))
        (for-each
          (lambda (sp)
            (let ((sourcedir (car sp)) (prefixlen (cdr sp)))
              (plog (string-append "processing " sourcedir))
              (walk-files sourcedir is-excluded?
                          (lambda (filepath) (process-file filepath prefixlen)))))
          source-prefixes)
        (set! completed #t))

      ;; deletion pass / state merge
      (if completed
          (begin
            (for-each
              (lambda (filepath)
                (unless (hash-table-exists? new-state filepath)
                  (let ((relpath (relpath-for filepath)))
                    (if (not relpath)
                        (begin
                          (plog (string-append "WARN: orphan state entry, not deleting: " filepath) 'both 'warn)
                          (hash-table-set! new-state filepath (hash-table-ref state filepath)))
                        (let ((full-destpath (norm-path (join-path dest-full relpath)))
                              (carry #f))
                          (when (path-exists? full-destpath)
                            (plog (string-append "-- " filepath " (full)") 'full 'file)
                            (if dry-run
                                (sinc! 'deleted)
                                (if (remove-file-pruning full-destpath dest-full)
                                    (sinc! 'deleted)
                                    (let ((err (string-append "failed to delete " full-destpath)))
                                      (add-error! 'full err) (plog err 'full 'warn)
                                      (sinc! 'failed)
                                      (hash-table-set! new-state filepath (hash-table-ref state filepath))
                                      (set! carry #t)))))
                          (unless carry
                            (let ((partial-destpath (norm-path (join-path dest-partial relpath))))
                              (when (path-exists? partial-destpath)
                                (plog (string-append "-- " filepath " (partial)") 'partial 'file)
                                (unless dry-run
                                  (unless (remove-file-pruning partial-destpath dest-partial)
                                    (let ((err (string-append "failed to delete " partial-destpath)))
                                      (add-error! 'partial err) (plog err 'partial 'warn))))))))))))
              (hash-table-keys state)))
          (begin
            ;; merge: keep old entries for paths we never reached.
            (for-each
              (lambda (filepath)
                (unless (hash-table-exists? new-state filepath)
                  (hash-table-set! new-state filepath (hash-table-ref state filepath))))
              (hash-table-keys state))
            (plog "WARN: backup did not complete; state merged, deletion pass skipped"))))

    (plog "write state")
    (unless dry-run
      (write-state-table (config-full-state-file config) new-state))

    (cond
      (dry-run #f)
      (completed
       (plog "copying state to partial folder")
       (guard (e (#t #f))
         (copy-file (config-full-state-file config) (join-path dest-partial "__state.json"))))
      (else
       (guard (e (#t #f))
         (call-with-output-file (join-path dest-partial "__incomplete")
           (lambda (p) (display (now-str) p))))))

    (let* ((elapsed (/ (- (timestamp) start-ms) 1000.0))
           (elapsed-rounded (/ (round (* elapsed 100)) 100.0))
           (error-count (+ (length errors-full) (length errors-partial)))
           (stats-alist
            (list (cons "new" (sget 'new))
                  (cons "changed" (sget 'changed))
                  (cons "deleted" (sget 'deleted))
                  (cons "unchanged" (sget 'unchanged))
                  (cons "failed" (sget 'failed))
                  (cons "bytes_copied" (sget 'bytes-copied))
                  (cons "elapsed_seconds" elapsed-rounded)
                  (cons "completed" completed)
                  (cons "dry_run" dry-run)
                  (cons "error_count" error-count))))
      (plog (string-append
             "summary: " (number->string (sget 'new)) " new, "
             (number->string (sget 'changed)) " changed, "
             (number->string (sget 'deleted)) " deleted, "
             (number->string (sget 'unchanged)) " unchanged, "
             (number->string (sget 'failed)) " failed, "
             (format-size (sget 'bytes-copied)) " copied in "
             (one-decimal elapsed) "s")
            'both 'summary)
      (plog "done")

      (when (pair? errors-full)
        (plog "Errors:" 'full 'warn)
        (for-each (lambda (e) (plog e 'full 'warn)) (reverse errors-full)))
      (when (pair? errors-partial)
        (plog "Errors:" 'partial 'warn)
        (for-each (lambda (e) (plog e 'partial 'warn)) (reverse errors-partial)))

      (close-output-port full-log-port)
      (close-output-port partial-log-port)

      (when json-out
        (json-write-pretty stats-alist (current-output-port))
        (newline))
      stats-alist)))

;; ======================================================================
;; restore
;; ======================================================================

(define (path-matches? fullpath patterns)
  (if (null? patterns)
      #t
      (any (lambda (pat)
             (if (or (string-index pat #\*) (string-index pat #\?) (string-index pat #\[))
                 (fnmatch? fullpath pat)
                 (string-prefix? pat fullpath)))
           patterns)))

(define (restore config destdir timestamp patterns dry-run force)
  (prn (string-append "restore" (if dry-run " (dry-run)" "")))
  (when (and (path-exists? destdir) (not force) (not dry-run))
    (prn (string-append "ERR: " destdir " exists, abort (use --force to merge into it)"))
    (exit 1))
  (let* ((partial-dir (config-dest-partial config))
         (history (filter
                    (lambda (h)
                      (and (string<=? h timestamp)
                           (directory-exists? (join-path partial-dir h))
                           (not (path-exists? (join-path partial-dir h "__incomplete")))))
                    (names-sorted-desc partial-dir))))
    (when (null? history)
      (prn (string-append "ERR: no usable snapshot at or before " timestamp))
      (exit 1))
    (let ((full-state (read-full-state-file (join-path partial-dir (car history) "__state.json")))
          (restored 0)
          (missing 0))
      (for-each
        (lambda (pr)
          (let ((fullpath (car pr)))
            (when (path-matches? fullpath patterns)
              (let ((prefix (find-source-prefix config fullpath)))
                (if (not prefix)
                    (prn (string-append "ERR: " fullpath " could not be matched to source dirs"))
                    (let* ((prefixlen (compute-prefixlen prefix))
                           (relpath (substring fullpath prefixlen (string-length fullpath))))
                      (let loop ((dirs history))
                        (cond
                          ((null? dirs)
                           (prn (string-append "ERR: " relpath " not found in backup"))
                           (set! missing (+ missing 1)))
                          (else
                           (let ((pathname (join-path partial-dir (car dirs) relpath)))
                             (if (path-exists? pathname)
                                 (let ((destpath (join-path destdir relpath)))
                                   (if dry-run
                                       (prn (string-append "DRY " destpath "  <-  " (car dirs) "/" relpath))
                                       (begin
                                         (make-directory (directory-name destpath))
                                         (copy-file pathname destpath)
                                         (prn destpath)))
                                   (set! restored (+ restored 1)))
                                 (loop (cdr dirs)))))))))))))
        full-state)
      (prn (string-append "done: " (number->string restored) " file(s) "
                          (if dry-run "would be " "") "restored"
                          (if (> missing 0) (string-append ", " (number->string missing) " missing") ""))))))

;; ======================================================================
;; package
;; ======================================================================

(define (parse-size s)
  (let* ((s (string-downcase s))
         (len (string-length s))
         (last (if (> len 0) (string-ref s (- len 1)) #\0)))
    (cond
      ((char=? last #\g) (* (string->number (substring s 0 (- len 1))) 1024 1024 1024))
      ((char=? last #\m) (* (string->number (substring s 0 (- len 1))) 1024 1024))
      ((char=? last #\k) (* (string->number (substring s 0 (- len 1))) 1024))
      (else (string->number s)))))

(define (package-data config destdir max-size timestamp full force)
  (prn "package-data")
  (let* ((cutoff
          (if full
              "0000-00-00"
              (let ((pkg-path (join-path (base-dir) (config-packaging-state-file config))))
                (if (path-exists? pkg-path)
                    (or (aget (read-json-file pkg-path) "timestamp") "0000-00-00")
                    "0000-00-00"))))
         (partial-dir (config-dest-partial config))
         (history (filter
                    (lambda (h)
                      (and (string<=? h timestamp) (string>? h cutoff)
                           (directory-exists? (join-path partial-dir h))
                           (not (path-exists? (join-path partial-dir h "__incomplete")))))
                    (names-sorted-desc partial-dir))))
    (if (null? history)
        (prn "done")
        (let ((full-state (read-full-state-file (join-path partial-dir (car history) "__state.json")))
              (index 1)
              (size 0))
          (define (destbase) (join-path destdir (string-append "backup-" timestamp "-part-" (number->string index))))
          (for-each
            (lambda (pr)
              (let ((fullpath (car pr)))
                (let ((prefix (find-source-prefix config fullpath)))
                  (if (not prefix)
                      (prn (string-append "ERR: " fullpath " could not be matched to source dirs"))
                      (let* ((prefixlen (compute-prefixlen prefix))
                             (relpath (string-replace-all (substring fullpath prefixlen (string-length fullpath)) "\\" "/")))
                        (let loop ((dirs history))
                          (unless (null? dirs)
                            (let ((pathname (join-path partial-dir (car dirs) relpath)))
                              (if (path-exists? pathname)
                                  (let ((filesize (file-size pathname)))
                                    (when (and (> size 0) (> (+ size filesize) max-size))
                                      (set! index (+ index 1))
                                      (set! size 0))
                                    (set! size (+ size filesize))
                                    (let ((destpath (join-path (destbase) relpath)))
                                      (unless (path-exists? destpath)
                                        (make-directory (directory-name destpath))
                                        (copy-file pathname destpath)
                                        (prn destpath))))
                                  (loop (cdr dirs)))))))))))
            full-state)
          (when full
            (let ((pkg-path (join-path (base-dir) (config-packaging-state-file config))))
              (call-with-output-file pkg-path
                (lambda (p) (json-write (list (cons "timestamp" timestamp)) p)))))
          (prn "done")))))

;; ======================================================================
;; init / config
;; ======================================================================

(define CONFIG-TEMPLATE
  (list (cons "source"
              (list (cons "directories" (vector "/path/to/source"))
                    (cons "excludes" #())))
        (cons "destination"
              (list (cons "directory_full" "/path/to/backup/full")
                    (cons "directory_partial" "/path/to/backup/partial")))
        (cons "full_state_file" "/path/to/state.json")
        (cons "packaging_state_file" "packaging-state.json")))

(define (cmd-init name force)
  (let ((target (join-path (base-dir) name)))
    (if (and (path-exists? target) (not force))
        (begin (prn (string-append "ERR: " target " exists (use --force to overwrite)")) (exit 1))
        (begin
          (call-with-output-file target
            (lambda (p) (json-write-pretty CONFIG-TEMPLATE p) (newline p)))
          (prn (string-append "wrote " target))
          (prn "Edit it to point at your sources and backup destinations, then run: scm dabbak.scm backup")))))

(define (cmd-config config)
  (json-write-pretty config (current-output-port))
  (newline))

;; ======================================================================
;; list (enumerate snapshots)
;; ======================================================================

(define (enumerate-snapshots partial-dir)
  (if (not (directory-exists? partial-dir))
      '()
      (filter-map
        (lambda (name)
          (let ((full (join-path partial-dir name)))
            (and (directory-exists? full)
                 (valid-date-string? name)
                 (let ((files 0) (bytes-total 0))
                   (walk-files full (lambda (p) #f)
                               (lambda (path)
                                 (let ((base (base-name path)))
                                   (unless (or (string=? base "__state.json") (string=? base "__incomplete"))
                                     (set! files (+ files 1))
                                     (set! bytes-total (+ bytes-total (guard (e (#t 0)) (file-size path))))))))
                   (list (cons "date" name)
                         (cons "path" full)
                         (cons "file_count" files)
                         (cons "total_bytes" bytes-total)
                         (cons "incomplete" (path-exists? (join-path full "__incomplete")))
                         (cons "log" (join-path partial-dir (string-append "backup-partial-" name ".log"))))))))
        (names-sorted-desc partial-dir))))

(define (snap-field s key) (aget s key))

(define (cmd-list config json-out)
  (let ((snaps (enumerate-snapshots (norm-path (config-dest-partial config)))))
    (cond
      (json-out
       (json-write-pretty
        (list->vector (map (lambda (s) (filter (lambda (kv) (not (string=? (car kv) "path"))) s)) snaps))
        (current-output-port))
       (newline))
      ((null? snaps) (prn "no snapshots"))
      (else
       (prn (string-pad-right "date" 12) " " (string-pad "files" 10) " " (string-pad "size" 10) "  status")
       (for-each
         (lambda (s)
           (let ((status (if (snap-field s "incomplete") "incomplete" "ok")))
             (prn (string-pad-right (snap-field s "date") 12) " "
                  (string-pad (group-digits (snap-field s "file_count")) 10) " "
                  (string-pad (format-size (snap-field s "total_bytes")) 10) "  " status)))
         snaps)))))

;; ======================================================================
;; prune
;; ======================================================================

(define (date->seconds s) (parse-iso8601 s))

(define (select-snapshots-to-prune snaps keep-last keep-days today)
  (let* ((today-sec (date->seconds today))
         (cutoff-sec (and keep-days (- today-sec (* keep-days 86400)))))
    (let loop ((ss snaps) (i 0) (acc '()))
      (if (null? ss)
          (reverse acc)
          (let* ((s (car ss)) (date (snap-field s "date")))
            (if (string=? date today)
                (loop (cdr ss) (+ i 1) acc)
                (let ((keep #f))
                  (when (and keep-last (< i keep-last)) (set! keep #t))
                  (when cutoff-sec
                    (let ((sec (date->seconds date)))
                      (if (and sec (>= sec cutoff-sec)) (set! keep #t))))
                  (loop (cdr ss) (+ i 1) (if keep acc (cons s acc))))))))))

(define (do-prune config keep-last keep-days force json-out)
  (let* ((partial (norm-path (config-dest-partial config)))
         (snaps (enumerate-snapshots partial))
         (to-delete (select-snapshots-to-prune snaps keep-last keep-days (today-str)))
         (del-dates (map (lambda (s) (snap-field s "date")) to-delete))
         (kept (filter-map (lambda (s) (and (not (member (snap-field s "date") del-dates))
                                            (snap-field s "date"))) snaps))
         (deleted '()))
    (if (not force)
        (begin
          (unless json-out
            (prn (string-append "DRY RUN (no --force). Would delete "
                                (number->string (length to-delete)) " snapshot(s):"))
            (for-each
              (lambda (s)
                (prn (string-append "  " (snap-field s "date") "  "
                                    (group-digits (snap-field s "file_count")) " files  "
                                    (format-size (snap-field s "total_bytes")))))
              to-delete)
            (prn (string-append "Would keep " (number->string (- (length snaps) (length to-delete))) " snapshot(s).")))
          (when json-out (print-prune-json kept '() del-dates force)))
        (begin
          (for-each
            (lambda (s)
              (guard (e (#t (unless json-out
                              (prn (string-append "ERR: failed to remove " (snap-field s "path") ": " (err->string e))))))
                (delete-directory (snap-field s "path"))
                (when (path-exists? (snap-field s "log"))
                  (guard (e2 (#t #f)) (delete-file (snap-field s "log"))))
                (set! deleted (cons (snap-field s "date") deleted))
                (unless json-out (prn (string-append "deleted " (snap-field s "date"))))))
            to-delete)
          (when json-out (print-prune-json kept (reverse deleted) del-dates force))))))

(define (print-prune-json kept deleted would-delete force)
  (json-write-pretty
   (list (cons "kept" (list->vector kept))
         (cons "deleted" (list->vector deleted))
         (cons "would_delete" (list->vector would-delete))
         (cons "force" force))
   (current-output-port))
  (newline))

;; ======================================================================
;; refresh-state
;; ======================================================================

(define (refresh-state config)
  (prn "refresh-state")
  (let* ((source-dirs (expand-source-dirs (config-source-directories config)))
         (dest-full (norm-path (config-dest-full config)))
         (dest-partial-base (norm-path (config-dest-partial config)))
         (timestamp (today-str))
         (dest-partial (join-path dest-partial-base timestamp))
         (new-state (make-hash-table)))
    (for-each
      (lambda (sourcedir)
        (let* ((prefix (directory-name sourcedir))
               (prefixlen (compute-prefixlen prefix))
               (destdir (join-path dest-full (substring sourcedir prefixlen (string-length sourcedir)))))
          (when (directory-exists? destdir)
            (let ((destdir-prefixlen (compute-prefixlen destdir)))
              (walk-files destdir (lambda (p) #f)
                          (lambda (filepath)
                            (let ((srcpath (join-path sourcedir (substring filepath destdir-prefixlen (string-length filepath)))))
                              (hash-table-set! new-state srcpath
                                               (vector (file-size filepath)
                                                       (quotient (file-modification-timestamp filepath) 1000))))))))))
      source-dirs)
    (write-state-table (config-full-state-file config) new-state)
    (when (directory-exists? dest-partial)
      (guard (e (#t #f))
        (copy-file (config-full-state-file config) (join-path dest-partial "__state.json"))))
    (prn "done")))

;; ======================================================================
;; CLI
;; ======================================================================

(define (cmd-backup config rest)
  (let loop ((args rest) (dry #f) (quiet #f) (json #f))
    (if (null? args)
        (with-lock config (lambda () (make-backup config dry quiet json)))
        (let ((a (car args)))
          (cond
            ((string=? a "--dry-run") (loop (cdr args) #t quiet json))
            ((or (string=? a "--quiet") (string=? a "-q")) (loop (cdr args) dry #t json))
            ((string=? a "--json") (loop (cdr args) dry quiet #t))
            (else (eprint "backup: unknown argument " a) (exit 2)))))))

(define (cmd-restore config rest)
  (let loop ((args rest) (dest #f) (ts #f) (dry #f) (force #f) (pos '()))
    (if (null? args)
        (let* ((patterns (reverse pos))
               (date-first (and (not ts) (pair? patterns) (valid-date-string? (car patterns))))
               (ts2 (if date-first (car patterns) ts))
               (patterns2 (if date-first (cdr patterns) patterns)))
          (if (not dest)
              (begin (eprint "restore: dest_dir is required") (exit 2))
              (restore config dest (or ts2 (today-str)) patterns2 dry force)))
        (let ((a (car args)))
          (cond
            ((or (string=? a "-t") (string=? a "--timestamp"))
             (if (null? (cdr args)) (begin (eprint "restore: " a " needs a value") (exit 2))
                 (loop (cddr args) dest (cadr args) dry force pos)))
            ((string=? a "--dry-run") (loop (cdr args) dest ts #t force pos))
            ((string=? a "--force") (loop (cdr args) dest ts dry #t pos))
            ((string-prefix? "--" a) (eprint "restore: unknown option " a) (exit 2))
            ((not dest) (loop (cdr args) a ts dry force pos))
            (else (loop (cdr args) dest ts dry force (cons a pos))))))))

(define (cmd-package config rest)
  (let loop ((args rest) (pos '()) (full #f) (force #f))
    (if (null? args)
        (let ((positional (reverse pos)))
          (if (< (length positional) 2)
              (begin (eprint "package: dest_dir and max_size are required") (exit 2))
              (let ((destdir (list-ref positional 0))
                    (max-size (parse-size (list-ref positional 1)))
                    (timestamp (if (>= (length positional) 3) (list-ref positional 2) (today-str))))
                ;; dest-exists check happens BEFORE acquiring the lock: a thunk
                ;; that calls (exit) under with-lock would leak the lock file.
                (if (and (path-exists? destdir) (not force))
                    (begin (prn "package-data")
                           (prn (string-append "ERR: " destdir " exists, abort"))
                           (exit 1))
                    (with-lock config (lambda () (package-data config destdir max-size timestamp full force)))))))
        (let ((a (car args)))
          (cond
            ((string=? a "--full") (loop (cdr args) pos #t force))
            ((string=? a "--force") (loop (cdr args) pos full #t))
            ((string-prefix? "--" a) (eprint "package: unknown option " a) (exit 2))
            (else (loop (cdr args) (cons a pos) full force)))))))

(define (cmd-prune config rest)
  (let loop ((args rest) (kl #f) (kd #f) (force #f) (json #f))
    (if (null? args)
        (if (and (not kl) (not kd))
            (begin (eprint "prune: must specify --keep-last and/or --keep-days") (exit 2))
            (if force
                (with-lock config (lambda () (do-prune config kl kd #t json)))
                (do-prune config kl kd #f json)))
        (let ((a (car args)))
          (cond
            ((string=? a "--keep-last")
             (if (null? (cdr args)) (begin (eprint "prune: --keep-last needs a value") (exit 2))
                 (loop (cddr args) (string->number (cadr args)) kd force json)))
            ((string=? a "--keep-days")
             (if (null? (cdr args)) (begin (eprint "prune: --keep-days needs a value") (exit 2))
                 (loop (cddr args) kl (string->number (cadr args)) force json)))
            ((string=? a "--force") (loop (cdr args) kl kd #t json))
            ((string=? a "--json") (loop (cdr args) kl kd force #t))
            (else (eprint "prune: unknown argument " a) (exit 2)))))))

(define (cmd-list-cmd config rest)
  (let loop ((args rest) (json #f))
    (if (null? args)
        (cmd-list config json)
        (let ((a (car args)))
          (cond
            ((string=? a "--json") (loop (cdr args) #t))
            (else (eprint "list: unknown argument " a) (exit 2)))))))

(define (cmd-init-cmd rest)
  (let loop ((args rest) (name "backup-config.json") (force #f))
    (if (null? args)
        (cmd-init name force)
        (let ((a (car args)))
          (cond
            ((string=? a "--name")
             (if (null? (cdr args)) (begin (eprint "init: --name needs a value") (exit 2))
                 (loop (cddr args) (cadr args) force)))
            ((string=? a "--force") (loop (cdr args) name #t))
            (else (eprint "init: unknown argument " a) (exit 2)))))))

(define (usage)
  (eprint "usage: scm dabbak.scm <command> [options]")
  (eprint "commands: init backup restore list prune package refresh-state config"))

(define (main argv)
  (if (null? argv)
      (begin (usage) (exit 2))
      (let ((cmd (car argv)) (rest (cdr argv)))
        (cond
          ((string=? cmd "init") (cmd-init-cmd rest))
          ((string=? cmd "gui")
           (eprint "gui is not available in the Scheme port; use the legacy-python branch for the Tkinter GUI")
           (exit 1))
          (else
           (let ((config (read-config)))
             (cond
               ((string=? cmd "backup") (cmd-backup config rest))
               ((string=? cmd "restore") (cmd-restore config rest))
               ((string=? cmd "package") (cmd-package config rest))
               ((string=? cmd "refresh-state") (with-lock config (lambda () (refresh-state config))))
               ((string=? cmd "config") (cmd-config config))
               ((string=? cmd "list") (cmd-list-cmd config rest))
               ((string=? cmd "prune") (cmd-prune config rest))
               (else (eprint "error: unknown command " cmd) (usage) (exit 2)))))))))

(main (cdr (command-line)))
