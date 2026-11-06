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
;;   init | backup | restore | list | prune | squash | package |
;;   refresh-state | config
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
        (scheme file)
        (scheme process-context)
        (scheme write)
        (srfi 1)
        (except (srfi 13) string-hash)
        (srfi 69)
        (srfi 132)
        (scm datetime)
        (scm fs)
        (scm json simple)
        (scm string)
        (scm system))

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
  (if (string? x)
      x
      (let ((p (open-output-string)))
        (display x p)
        (get-output-string p))))

(define (err->string e)
  (if (error-object? e) (error-object-message e) (->string e)))

;; python normpath: collapse, then strip a single trailing separator.
(define (norm-path p)
  (let* ((n (normalized-path p))
         (len (string-length n)))
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
                (begin
                  (write-char c o)
                  (loop)))))))))

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
  (let* ((s (number->string n))
         (len (string-length s)))
    (let loop ((i 0) (out '()))
      (if (>= i len)
          (list->string (reverse out))
          (let ((rem (- len i)))
            (loop (+ i 1)
                  (cons (string-ref s i)
                        (if (and (> i 0) (= (modulo rem 3) 0))
                            (cons #\, out)
                            out))))))))

(define (valid-date-string? s)
  (and (string? s)
       (string-matches s "^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$")
       #t))

(define (today-str) (today)) ;; "YYYY-MM-DD" (local)
(define (now-str) (now 'iso)) ;; "YYYY-MM-DD HH:MM" (local)

;; ======================================================================
;; fnmatch (gitignore-flavored), implemented via fnmatch->regex.
;; fnmatch '*' matches path separators (unlike a typical glob), matching
;; python's fnmatch used by dabbak's exclude/pattern logic.
;; ======================================================================

(define (regex-special? c)
  (memv c '(#\. #\^ #\$ #\+ #\{ #\} #\( #\) #\| #\\ #\])))

(define (fnmatch->regex pat)
  (let ((out (open-output-string))
        (len (string-length pat)))
    (write-char #\^ out)
    (let loop ((i 0))
      (if (>= i len)
          (begin
            (write-char #\$ out)
            (get-output-string out))
          (let ((c (string-ref pat i)))
            (cond ((char=? c #\*) (write-string ".*" out) (loop (+ i 1)))
                  ((char=? c #\?) (write-char #\. out) (loop (+ i 1)))
                  ((char=? c #\[)
                   (let ((j (class-end pat (+ i 1) len)))
                     (if (not j)
                         (begin
                           (write-string "\\[" out)
                           (loop (+ i 1)))
                         (begin
                           (write-char #\[ out)
                           (let* ((neg (and
                                        (< (+ i 1) len)
                                        (char=? (string-ref pat (+ i 1))
                                                #\!)))
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
                  ((regex-special? c)
                   (write-char #\\ out)
                   (write-char c out)
                   (loop (+ i 1)))
                  (else (write-char c out) (loop (+ i 1)))))))))

;; find the index of the ']' closing a class started at `start` (the char
;; after '['). A ']' as the first class char is literal. #f if unterminated.
(define (class-end pat start len)
  (let
      ((first (if (and (< start len) (char=? (string-ref pat start) #\!))
                  (+ start 1)
                  start)))
    (let loop
        ((i (if (and (< first len) (char=? (string-ref pat first) #\]))
                (+ first 1)
                first)))
      (cond ((>= i len) #f)
            ((char=? (string-ref pat i) #\]) i)
            (else (loop (+ i 1)))))))

(define (fnmatch? name pat)
  (let ((nm (if *windows?* (string-downcase name) name))
        (pt (if *windows?* (string-downcase pat) pat)))
    (and (string-matches nm (fnmatch->regex pt)) #t)))

(define (has-glob? s)
  (or (string-index s #\*) (string-index s #\?) (string-index s #\[)))

(define (has-slash? s) (or (string-index s #\/) (string-index s #\\)))

;; compile a list of exclude entries into a (path -> boolean) predicate.
(define (compile-excludes excludes)
  (let loop ((es excludes) (basenames '()) (fullpaths '()) (abspaths '()))
    (if (null? es)
        (let ((basenames (reverse basenames))
              (fullpaths (reverse fullpaths)))
          (lambda (path)
            (or (any (lambda (a)
                       (or (string=? a path) (string=? a (norm-path path))))
                     abspaths)
                (let ((base (base-name path)))
                  (any (lambda (p) (fnmatch? base p)) basenames))
                (any (lambda (p) (fnmatch? path p)) fullpaths))))
        (let ((raw (car es)))
          (cond ((not (has-slash? raw))
                 (loop (cdr es) (cons raw basenames) fullpaths abspaths))
                ((has-glob? raw)
                 (loop (cdr es) basenames (cons raw fullpaths) abspaths))
                (else (loop (cdr es)
                            basenames
                            fullpaths
                            (cons (norm-path raw) abspaths))))))))

;; ======================================================================
;; walk — recursive file traversal, sorted, skipping symlinks/excludes.
;; calls (proc filepath) for each file (mirrors the python generator).
;; ======================================================================

(define (walk-files directory excluded? proc)
  (cond ((file-exists? directory)
         (unless (excluded? directory) (proc directory)))
        ((excluded? directory) #f)
        ((directory-exists? directory)
         (for-each (lambda (entry)
                     (let ((full (join-path directory (car entry)))
                           (type (cdr entry)))
                       (cond ((excluded? full) #f)
                             ((eq? type 'symlink) #f) ;; skip symlinks (and junctions)
                             ((eq? type 'directory)
                              (walk-files full excluded? proc))
                             ((eq? type 'file) (proc full)))))
                   (sorted-entries directory)))
        (else #f)))

;; ======================================================================
;; config / state
;; ======================================================================

(define *script-path* (car (command-line)))
(define (base-dir) (directory-name (absolute-path *script-path*)))

(define (aget obj key) (json-ref obj key)) ;; object lookup via (scm json simple)

(define (config-source-directories c)
  (let ((d (aget (aget c "source") "directories")))
    (if (vector? d) (vector->list d) '())))
(define (config-source-excludes c)
  (let ((e (aget (aget c "source") "excludes")))
    (if (vector? e) (vector->list e) '())))
(define (config-is-windows c)
  (let ((w (aget (aget c "source") "is-windows"))) (and (boolean? w) w)))
(define (config-dest-full c) (aget (aget c "destination") "directory_full"))
(define (config-dest-partial c)
  (aget (aget c "destination") "directory_partial"))
(define (config-full-state-file c) (aget c "full_state_file"))
(define (config-packaging-state-file c) (aget c "packaging_state_file"))

(define (read-config)
  (let* ((cfgfile (or (get-environment-variable "DABBAK_CONFIG")
                      "backup-config.json"))
         (filepath (join-path (base-dir) cfgfile)))
    (if (path-exists? filepath)
        (read-json-file filepath)
        (begin
          (eprint "ERR: config not found: " filepath)
          (exit 1)))))

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
                 (md0 (if is-wild
                          (substring source-dir 0
                                     (- (string-length source-dir) 1))
                          source-dir))
                 (match-dir (rstrip-char md0 sepc)))
            (if (and (not (string=? fullpath match-dir))
                     (not (string-prefix? (string-append match-dir sep)
                                          fullpath)))
                (loop (cdr dirs))
                (if is-wild
                    match-dir
                    (let ((idx (string-index-right match-dir sepc)))
                      (if idx (substring match-dir 0 idx) match-dir)))))))))

(define (expand-source-dirs directories)
  (append-map (lambda (srcdir0)
                (let ((srcdir (norm-path srcdir0)))
                  (if (or (string-suffix? "*" srcdir)
                          (string-suffix? (string-append "*" *sep*) srcdir))
                      (let
                          ((base (rstrip-char (rstrip-char srcdir #\*)
                                              *sepchar*)))
                        (if (directory-exists? base)
                            (filter-map
                             (lambda (entry)
                               (and (eq? (cdr entry) 'directory)
                                    (join-path base (car entry))))
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

(define (lock-path-for config)
  (string-append (config-full-state-file config) ".lock"))

;; run thunk under the per-config write lock. Exits 1 if another process holds
;; it. The OS releases the lock on process exit, so even an (exit) inside thunk
;; cannot leave it stale; file-unlock is still called on the normal/error path.
(define (with-lock config thunk)
  (let* ((lp (lock-path-for config))
         (handle (file-lock lp)))
    (if handle
        (let
            ((result (guard (e (#t (file-unlock handle) (raise e))) (thunk))))
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
  (join-path dest-partial-base
             (string-append "backup-partial-" today ".log")))

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
      (when
          (and (string-prefix? stop-dir dirpath)
               (not (string=? dirpath stop-dir)))
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
         (pg-files 0)
         (pg-bytes 0)
         (pg-last 0))

    (define (sget k) (hash-table-ref/default stats k 0))
    (define (sset! k v) (hash-table-set! stats k v))
    (define (sinc! k) (sset! k (+ (sget k) 1)))
    (define (sadd! k n) (sset! k (+ (sget k) n)))

    ;; plog: log files always; stdout gated by quiet/json-out.
    (define (plog msg . rest)
      (let ((dest (if (pair? rest) (car rest) 'both))
            (level (if (and (pair? rest) (pair? (cdr rest)))
                       (cadr rest)
                       'info)))
        (when (memq dest '(full both))
          (display msg full-log-port)
          (newline full-log-port))
        (when (memq dest '(partial both))
          (display msg partial-log-port)
          (newline partial-log-port))
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
            (eprint "[" pg-files " files, "
                    (format-size pg-bytes)
                    "] "
                    path)))))

    (define (copy-into filepath destbase relpath overwrite tag)
      (let ((destpath (norm-path (join-path destbase relpath))))
        (if dry-run
            #t
            (guard (e (#t
                       (let
                           ((err (string-append "ERR: failed to copy "
                                                filepath " => " destpath)))
                         (add-error! tag err)
                         (plog err tag 'warn)
                         (plog (err->string e) tag 'warn))
                       #f))
              (make-directory (directory-name destpath))
              (when (and overwrite (path-exists? destpath))
                (delete-file destpath))
              (if (eq? (copy-file filepath destpath) #f)
                  (let
                      ((err (string-append "ERR: failed to copy " filepath
                                           " => " destpath)))
                    (add-error! tag err)
                    (plog err tag 'warn)
                    #f)
                  #t)))))

    (for-each (lambda (k) (sset! k 0))
              '(new changed deleted unchanged failed bytes-copied))

    (rotate-log-if-large (get-full-log))
    (when (and dry-run (not json-out))
      (prn "DRY RUN: no files will be copied, deleted, or state written"))

    (unless (path-exists? dest-partial-base)
      (make-directory dest-partial-base))

    (set! full-log-port (open-output-file (get-full-log) 'append))
    (set! partial-log-port
          (open-output-file (get-partial-log dest-partial-base today)
                            'append))

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
    (let
        ((source-prefixes (map (lambda (sd)
                                 (cons sd
                                       (compute-prefixlen
                                        (directory-name sd))))
                               source-dirs)))

      (define (relpath-for filepath)
        (let loop ((sps source-prefixes))
          (if (null? sps)
              #f
              (let ((sd (caar sps))
                    (plen (cdar sps)))
                (if (or (string=? filepath sd)
                        (string-prefix? (string-append sd *sep*) filepath))
                    (substring filepath plen (string-length filepath))
                    (loop (cdr sps)))))))

      (define (process-file filepath prefixlen)
        (let ((fsize (guard (e (#t #f)) (file-size filepath))))
          (if (not fsize)
              (let
                  ((err (string-append "ERR: file " filepath
                                       " not found (fstat)")))
                (add-error! 'full err)
                (add-error! 'partial err)
                (plog err 'both 'warn))
              (let* ((fsec (quotient (file-modification-timestamp filepath)
                                     1000))
                     (relpath (substring filepath prefixlen
                                         (string-length filepath)))
                     (entry (hash-table-ref/default state filepath #f)))
                (progress-tick! filepath fsize)
                (if entry
                    (let ((orig-size (vector-ref entry 0))
                          (orig-mtime (vector-ref entry 1)))
                      (if (or (not (= fsize orig-size))
                              (mtime-changed? fsec orig-mtime))
                          (begin
                            (plog (string-append "** " filepath) 'both 'file)
                            (let ((ok-p
                                   (copy-into filepath dest-partial relpath #t
                                              'partial))
                                  (ok-f
                                   (copy-into filepath dest-full relpath #t
                                              'full)))
                              (if (and ok-p ok-f)
                                  (begin
                                    (sinc! 'changed)
                                    (sadd! 'bytes-copied fsize)
                                    (hash-table-set! new-state filepath
                                                     (vector fsize fsec)))
                                  (begin
                                    (hash-table-set!
                                     new-state
                                     filepath
                                     (vector orig-size orig-mtime))
                                    (sinc! 'failed)))))
                          (begin
                            (sinc! 'unchanged)
                            (hash-table-set! new-state filepath
                                             (vector fsize fsec)))))
                    (begin
                      (plog (string-append "++ " filepath) 'both 'file)
                      (let ((ok-p (copy-into filepath dest-partial relpath #f
                                             'partial))
                            (ok-f (copy-into filepath dest-full relpath #f
                                             'full)))
                        (if (and ok-p ok-f)
                            (begin
                              (sinc! 'new)
                              (sadd! 'bytes-copied fsize)
                              (hash-table-set! new-state filepath
                                               (vector fsize fsec)))
                            (sinc! 'failed)))))))))

      ;; walk pass — guard catches any error (incl. interrupt) and leaves
      ;; completed=#f so the deletion pass is skipped and state is merged.
      (guard
          (e (#t
              (plog (string-append "interrupted by exception: "
                                   (err->string e))
                    'both
                    'warn)))
        (for-each (lambda (sp)
                    (let ((sourcedir (car sp))
                          (prefixlen (cdr sp)))
                      (plog (string-append "processing " sourcedir))
                      (walk-files sourcedir is-excluded?
                                  (lambda (filepath)
                                    (process-file filepath prefixlen)))))
                  source-prefixes)
        (set! completed #t))

      ;; deletion pass / state merge
      (if completed
          (begin
            (for-each (lambda (filepath)
                        (unless (hash-table-exists? new-state filepath)
                          (let ((relpath (relpath-for filepath)))
                            (if (not relpath)
                                (begin
                                  (plog
                                   (string-append
                                    "WARN: orphan state entry, not deleting: "
                                    filepath)
                                   'both
                                   'warn)
                                  (hash-table-set!
                                   new-state
                                   filepath
                                   (hash-table-ref state filepath)))
                                (let ((full-destpath
                                       (norm-path
                                        (join-path dest-full relpath)))
                                      (carry #f))
                                  (when (path-exists? full-destpath)
                                    (plog
                                     (string-append "-- " filepath " (full)")
                                     'full
                                     'file)
                                    (if
                                     dry-run
                                     (sinc! 'deleted)
                                     (if
                                      (remove-file-pruning full-destpath
                                                           dest-full)
                                      (sinc! 'deleted)
                                      (let
                                          ((err
                                            (string-append "failed to delete "
                                                           full-destpath)))
                                        (add-error! 'full err)
                                        (plog err 'full 'warn)
                                        (sinc! 'failed)
                                        (hash-table-set!
                                         new-state
                                         filepath
                                         (hash-table-ref state filepath))
                                        (set! carry #t)))))
                                  (unless carry
                                    (let
                                        ((partial-destpath
                                          (norm-path
                                           (join-path dest-partial relpath))))
                                      (when (path-exists? partial-destpath)
                                        (plog
                                         (string-append "-- " filepath
                                                        " (partial)")
                                         'partial
                                         'file)
                                        (unless dry-run
                                          (unless
                                              (remove-file-pruning
                                               partial-destpath dest-partial)
                                            (let
                                                ((err
                                                  (string-append
                                                   "failed to delete "
                                                   partial-destpath)))
                                              (add-error! 'partial err)
                                              (plog err
                                                    'partial
                                                    'warn))))))))))))
                      (hash-table-keys state)))
          (begin
            ;; merge: keep old entries for paths we never reached.
            (for-each (lambda (filepath)
                        (unless (hash-table-exists? new-state filepath)
                          (hash-table-set! new-state filepath
                                           (hash-table-ref state filepath))))
                      (hash-table-keys state))
            (plog
             "WARN: backup did not complete; state merged, deletion pass skipped"))))

    (plog "write state")
    (unless dry-run
      (write-state-table (config-full-state-file config) new-state))

    (cond (dry-run #f)
          (completed (plog "copying state to partial folder")
                     (guard (e (#t #f))
                       (copy-file (config-full-state-file config)
                                  (join-path dest-partial "__state.json"))))
          (else (guard (e (#t #f))
                  (call-with-output-file
                      (join-path dest-partial "__incomplete")
                    (lambda (p) (display (now-str) p))))))

    (let* ((elapsed (/ (- (timestamp) start-ms) 1000.0))
           (elapsed-rounded (/ (round (* elapsed 100)) 100.0))
           (error-count (+ (length errors-full) (length errors-partial)))
           (stats-alist (list (cons "new" (sget 'new))
                              (cons "changed" (sget 'changed))
                              (cons "deleted" (sget 'deleted))
                              (cons "unchanged" (sget 'unchanged))
                              (cons "failed" (sget 'failed))
                              (cons "bytes_copied" (sget 'bytes-copied))
                              (cons "elapsed_seconds" elapsed-rounded)
                              (cons "completed" completed)
                              (cons "dry_run" dry-run)
                              (cons "error_count" error-count))))
      (plog (string-append "summary: "
                           (number->string (sget 'new))
                           " new, "
                           (number->string (sget 'changed))
                           " changed, "
                           (number->string (sget 'deleted))
                           " deleted, "
                           (number->string (sget 'unchanged))
                           " unchanged, "
                           (number->string (sget 'failed))
                           " failed, "
                           (format-size (sget 'bytes-copied))
                           " copied in "
                           (one-decimal elapsed)
                           "s")
            'both
            'summary)
      (plog "done")

      (when (pair? errors-full)
        (plog "Errors:" 'full 'warn)
        (for-each (lambda (e) (plog e 'full 'warn)) (reverse errors-full)))
      (when (pair? errors-partial)
        (plog "Errors:" 'partial 'warn)
        (for-each (lambda (e) (plog e 'partial 'warn))
                  (reverse errors-partial)))

      (close-output-port full-log-port)
      (close-output-port partial-log-port)

      (when json-out
        (json-write-pretty stats-alist (current-output-port))
        (newline))
      stats-alist)))

;; ======================================================================
;; snapshot history (shared by restore / package / squash)
;; ======================================================================

;; marker / metadata files that live at the root of a dated snapshot dir.
;;   __state.json   full state as of this snapshot (complete runs only)
;;   __incomplete   the run was interrupted: not a restore point
;;   __squashed     being merged into a later snapshot: not a restore point,
;;                  but its files are still searched (crash-safe squash)
;;   __squash.json  journal on the squash TARGET: {target, sources}
(define SNAPSHOT-META-FILES
  '("__state.json" "__incomplete" "__squashed" "__squash.json"))

(define (snapshot-meta-file? name) (member name SNAPSHOT-META-FILES))

;; a snapshot that restore may use as its point in time (has a state, was
;; not interrupted, is not the source of a pending squash).
(define (snapshot-complete? dir)
  (and (file-exists? (join-path dir "__state.json"))
       (not (path-exists? (join-path dir "__incomplete")))
       (not (path-exists? (join-path dir "__squashed")))))

;; dated snapshot dir names under partial-dir, newest first.
(define (dated-snapshot-names partial-dir)
  (filter (lambda (h)
            (and (valid-date-string? h)
                 (directory-exists? (join-path partial-dir h))))
          (names-sorted-desc partial-dir)))

;; (point . search) for a restore at `timestamp` over snapshots newer than
;; `cutoff`: point = newest complete snapshot <= timestamp, search = every
;; dated dir in (cutoff, point], newest first. Incomplete and squash-source
;; dirs are searched for file copies (their files are versions the later
;; state references) but are never the point itself. #f if no point.
(define (snapshot-history partial-dir timestamp cutoff)
  (let* ((dirs (filter (lambda (h)
                         (and (string<=? h timestamp) (string>? h cutoff)))
                       (dated-snapshot-names partial-dir)))
         (point (find (lambda (h)
                        (snapshot-complete? (join-path partial-dir h)))
                      dirs)))
    (and point (cons point (filter (lambda (h) (string<=? h point)) dirs)))))

;; ======================================================================
;; restore
;; ======================================================================

(define (path-matches? fullpath patterns)
  (if (null? patterns)
      #t
      (any (lambda (pat)
             (if (or (string-index pat #\*)
                     (string-index pat #\?)
                     (string-index pat #\[))
                 (fnmatch? fullpath pat)
                 (string-prefix? pat fullpath)))
           patterns)))

(define (restore config destdir timestamp patterns dry-run force)
  (prn (string-append "restore" (if dry-run " (dry-run)" "")))
  (when (and (path-exists? destdir) (not force) (not dry-run))
    (prn (string-append "ERR: " destdir
                        " exists, abort (use --force to merge into it)"))
    (exit 1))
  (let* ((partial-dir (config-dest-partial config))
         (hist (snapshot-history partial-dir timestamp "0000-00-00")))
    (unless hist
      (prn (string-append "ERR: no usable snapshot at or before " timestamp))
      (exit 1))
    (let ((history (cdr hist))
          (full-state (read-full-state-file
                       (join-path partial-dir (car hist) "__state.json")))
          (restored 0)
          (missing 0))
      (for-each (lambda (pr)
                  (let ((fullpath (car pr)))
                    (when (path-matches? fullpath patterns)
                      (let ((prefix (find-source-prefix config fullpath)))
                        (if (not prefix)
                            (prn (string-append "ERR: " fullpath
                                                " could not be matched to source dirs"))
                            (let* ((prefixlen (compute-prefixlen prefix))
                                   (relpath
                                    (substring fullpath prefixlen
                                               (string-length fullpath))))
                              (let loop ((dirs history))
                                (cond ((null? dirs)
                                       (prn
                                        (string-append "ERR: " relpath
                                                       " not found in backup"))
                                       (set! missing (+ missing 1)))
                                      (else
                                       (let
                                           ((pathname
                                             (join-path partial-dir
                                                        (car dirs)
                                                        relpath)))
                                         (if
                                          (path-exists? pathname)
                                          (let
                                              ((destpath
                                                (join-path destdir relpath)))
                                            (if
                                             dry-run
                                             (prn
                                              (string-append "DRY " destpath
                                                             "  <-  "
                                                             (car dirs)
                                                             "/"
                                                             relpath))
                                             (begin
                                               (make-directory
                                                (directory-name destpath))
                                               (copy-file pathname destpath)
                                               (prn destpath)))
                                            (set! restored (+ restored 1)))
                                          (loop (cdr dirs)))))))))))))
                full-state)
      (prn (string-append "done: "
                          (number->string restored)
                          " file(s) "
                          (if dry-run "would be " "")
                          "restored"
                          (if (> missing 0)
                              (string-append ", "
                                             (number->string missing)
                                             " missing")
                              ""))))))

;; ======================================================================
;; package
;; ======================================================================

(define (parse-size s)
  (let* ((s (string-downcase s))
         (len (string-length s))
         (last (if (> len 0) (string-ref s (- len 1)) #\0)))
    (cond ((char=? last #\g)
           (* (string->number (substring s 0 (- len 1))) 1024 1024 1024))
          ((char=? last #\m)
           (* (string->number (substring s 0 (- len 1))) 1024 1024))
          ((char=? last #\k)
           (* (string->number (substring s 0 (- len 1))) 1024))
          (else (string->number s)))))

(define (package-data config destdir max-size timestamp full force)
  (prn "package-data")
  (let* ((cutoff (if full
                     "0000-00-00"
                     (let
                         ((pkg-path (join-path
                                     (base-dir)
                                     (config-packaging-state-file config))))
                       (if (path-exists? pkg-path)
                           (or (aget (read-json-file pkg-path) "timestamp")
                               "0000-00-00")
                           "0000-00-00"))))
         (partial-dir (config-dest-partial config))
         (hist (snapshot-history partial-dir timestamp cutoff)))
    (if (not hist)
        (prn "done")
        (let ((history (cdr hist))
              (full-state (read-full-state-file
                           (join-path partial-dir (car hist) "__state.json")))
              (index 1)
              (size 0))
          (define (destbase)
            (join-path destdir
                       (string-append "backup-" timestamp "-part-"
                                      (number->string index))))
          (for-each (lambda (pr)
                      (let ((fullpath (car pr)))
                        (let ((prefix (find-source-prefix config fullpath)))
                          (if (not prefix)
                              (prn (string-append "ERR: " fullpath
                                                  " could not be matched to source dirs"))
                              (let* ((prefixlen (compute-prefixlen prefix))
                                     (relpath
                                      (string-replace-all
                                       (substring fullpath prefixlen
                                                  (string-length fullpath))
                                       "\\"
                                       "/")))
                                (let loop ((dirs history))
                                  (unless (null? dirs)
                                    (let
                                        ((pathname
                                          (join-path partial-dir
                                                     (car dirs)
                                                     relpath)))
                                      (if
                                       (path-exists? pathname)
                                       (let ((filesize (file-size pathname)))
                                         (when
                                             (and
                                              (> size 0)
                                              (> (+ size filesize) max-size))
                                           (set! index (+ index 1))
                                           (set! size 0))
                                         (set! size (+ size filesize))
                                         (let
                                             ((destpath
                                               (join-path (destbase)
                                                          relpath)))
                                           (unless (path-exists? destpath)
                                             (make-directory
                                              (directory-name destpath))
                                             (copy-file pathname destpath)
                                             (prn destpath))))
                                       (loop (cdr dirs)))))))))))
                    full-state)
          (when full
            (let
                ((pkg-path (join-path (base-dir)
                                      (config-packaging-state-file config))))
              (call-with-output-file pkg-path
                (lambda (p)
                  (json-write (list (cons "timestamp" timestamp)) p)))))
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
        (begin
          (prn (string-append "ERR: " target
                              " exists (use --force to overwrite)"))
          (exit 1))
        (begin
          (call-with-output-file target
            (lambda (p)
              (json-write-pretty CONFIG-TEMPLATE p)
              (newline p)))
          (prn (string-append "wrote " target))
          (prn
           "Edit it to point at your sources and backup destinations, then run: scm dabbak.scm backup")))))

(define (cmd-config config)
  (json-write-pretty config (current-output-port))
  (newline))

;; ======================================================================
;; list (enumerate snapshots)
;; ======================================================================

(define (enumerate-snapshots partial-dir)
  (if (not (directory-exists? partial-dir))
      '()
      (filter-map (lambda (name)
                    (let ((full (join-path partial-dir name)))
                      (and (directory-exists? full)
                           (valid-date-string? name)
                           (let ((files 0) (bytes-total 0))
                             (walk-files
                              full
                              (lambda (p) #f)
                              (lambda (path)
                                (let ((base (base-name path)))
                                  (unless
                                      (and
                                       (string=? (directory-name path) full)
                                       (snapshot-meta-file? base))
                                    (set! files (+ files 1))
                                    (set!
                                     bytes-total
                                     (+
                                      bytes-total
                                      (guard (e (#t 0))
                                        (file-size path))))))))
                             (list (cons "date" name)
                                   (cons "path" full)
                                   (cons "file_count" files)
                                   (cons "total_bytes" bytes-total)
                                   (cons
                                    "incomplete"
                                    (path-exists?
                                     (join-path full "__incomplete")))
                                   (cons
                                    "log"
                                    (join-path
                                     partial-dir
                                     (string-append "backup-partial-" name
                                                    ".log"))))))))
                  (names-sorted-desc partial-dir))))

(define (snap-field s key) (aget s key))

(define (cmd-list config json-out)
  (let
      ((snaps (enumerate-snapshots (norm-path (config-dest-partial config)))))
    (cond (json-out (json-write-pretty (list->vector
                                        (map
                                         (lambda (s)
                                           (filter
                                            (lambda (kv)
                                              (not
                                               (string=? (car kv) "path")))
                                            s))
                                         snaps))
                                       (current-output-port))
                    (newline))
          ((null? snaps) (prn "no snapshots"))
          (else (prn (string-pad-right "date" 12)
                     " "
                     (string-pad "files" 10)
                     " "
                     (string-pad "size" 10)
                     "  status")
                (for-each (lambda (s)
                            (let ((status
                                   (let ((dir (snap-field s "path")))
                                     (cond
                                      ((path-exists?
                                        (join-path dir "__squashed"))
                                       "squash pending (source)")
                                      ((path-exists?
                                        (join-path dir "__squash.json"))
                                       "squash pending (target)")
                                      ((snap-field s "incomplete")
                                       "incomplete")
                                      (else "ok")))))
                              (prn (string-pad-right (snap-field s "date") 12)
                                   " "
                                   (string-pad
                                    (group-digits (snap-field s "file_count"))
                                    10)
                                   " "
                                   (string-pad
                                    (format-size (snap-field s "total_bytes"))
                                    10)
                                   "  "
                                   status)))
                          snaps)))))

;; ======================================================================
;; squash (merge-forward) — the engine behind `squash` and `prune`
;;
;; Removing snapshots R in favour of a later complete snapshot K keeps every
;; restore point >= K identical: restore takes, per path, the newest copy at
;; or before its point. So, walking R newest-first, each file S/rel is
;;   - dropped if K already has rel (K's copy, or one moved earlier, is newer)
;;   - moved to K/rel if rel is still in K's __state.json
;;   - dropped otherwise (deleted from the source before K: no restore >= K
;;     asks for it, and a reappearing file is copied afresh by backup)
;; Crash safety: K/__squash.json (the job) is written and every source gets
;; a __squashed marker before anything moves. Sources stop being restore
;; points but are still searched, so a restore at K is correct at every
;; intermediate step. An interrupted job is finished by the next
;; `squash` / `prune --force`.
;; ======================================================================

(define (slashify s) (string-replace-all s "\\" "/"))

;; hash set of "/"-separated relpaths referenced by K's __state.json.
(define (snapshot-relpath-set config k-dir)
  (let ((set (make-hash-table)))
    (for-each (lambda (pr)
                (let* ((fullpath (car pr))
                       (prefix (find-source-prefix config fullpath)))
                  (when prefix
                    (hash-table-set! set
                                     (slashify
                                      (substring fullpath
                                                 (compute-prefixlen prefix)
                                                 (string-length fullpath)))
                                     #t))))
              (read-full-state-file (join-path k-dir "__state.json")))
    set))

(define (make-squash-stats)
  (let ((h (make-hash-table)))
    (for-each (lambda (k) (hash-table-set! h k 0))
              '(moved dropped bytes-freed errors))
    h))
(define (stat-ref st k) (hash-table-ref/default st k 0))
(define (stat-add! st k n) (hash-table-set! st k (+ (stat-ref st k) n)))

;; move one file, falling back to copy + delete. #t on success.
(define (move-or-copy src dst)
  (guard (e (#t
             (guard (e2 (#t #f))
               (copy-file src dst)
               (delete-file src)
               #t)))
    (move-file src dst)
    #t))

;; merge source snapshot dir s-dir into k-dir. `claimed` holds the relpaths
;; already present in / moved into K (it also makes dry-run counts exact).
;; s-dir is deleted afterwards unless a file could not be handled.
(define (merge-snapshot-into! s-dir k-dir relset claimed dry-run stats)
  (let ((s-prefixlen (compute-prefixlen s-dir))
        (errors 0))
    (define (drop! path size)
      (stat-add! stats 'dropped 1)
      (stat-add! stats 'bytes-freed size)
      (unless dry-run
        (unless (guard (e (#t #f))
                  (delete-file path)
                  #t)
          (prn (string-append "ERR: failed to delete " path))
          (set! errors (+ errors 1)))))
    (walk-files s-dir
                (lambda (p)
                  (and (string=? (directory-name p) s-dir)
                       (snapshot-meta-file? (base-name p))))
                (lambda (path)
                  (let* ((rel (substring path s-prefixlen
                                         (string-length path)))
                         (key (slashify rel))
                         (dst (join-path k-dir rel))
                         (size (guard (e (#t 0)) (file-size path))))
                    (cond ((or (hash-table-exists? claimed key)
                               (path-exists? dst))
                           (hash-table-set! claimed key #t)
                           (drop! path size))
                          ((hash-table-exists? relset key)
                           (hash-table-set! claimed key #t)
                           (stat-add! stats 'moved 1)
                           (unless dry-run
                             (make-directory (directory-name dst))
                             (unless (move-or-copy path dst)
                               (prn (string-append "ERR: failed to move " path
                                                   " -> " dst))
                               (hash-table-delete! claimed key)
                               (set! errors (+ errors 1)))))
                          (else (drop! path size))))))
    (unless (or dry-run (> errors 0))
      (guard
          (e (#t
              (prn (string-append "ERR: failed to remove " s-dir ": "
                                  (err->string e)))
              (set! errors (+ errors 1))))
        (delete-directory s-dir)))
    (stat-add! stats 'errors errors)
    (= errors 0)))

;; prepend the sources' partial logs (oldest first) to the target's log so
;; the history of what was backed up when is not lost, then drop them.
(define (merge-snapshot-logs! partial-dir target sources)
  (let* ((k-log (get-partial-log partial-dir target))
         (src-logs (filter (lambda (pr) (file-exists? (cdr pr)))
                           (map (lambda (d)
                                  (cons d (get-partial-log partial-dir d)))
                                (list-sort string<? sources)))))
    (when (pair? src-logs)
      (guard
          (e (#t
              (prn (string-append "WARN: could not merge logs into " k-log
                                  ": "
                                  (err->string e)))))
        (let ((tmp (string-append k-log ".tmp")))
          (call-with-output-file tmp
            (lambda (p)
              (for-each (lambda (pr)
                          (display (string-append "== squashed from "
                                                  (car pr)
                                                  " ==\n")
                                   p)
                          (display (read-file->string (cdr pr)) p))
                        src-logs)
              (when (file-exists? k-log)
                (display (string-append "== " target " ==\n") p)
                (display (read-file->string k-log) p))))
          (move-file tmp k-log)
          (for-each (lambda (pr) (delete-file (cdr pr))) src-logs))))))

;; run (or resume) one job: merge `sources` (dates) into `target` (date).
;; Returns the stats table.
(define (run-squash-job! config target sources dry-run)
  (let* ((partial-dir (norm-path (config-dest-partial config)))
         (k-dir (join-path partial-dir target))
         (journal (join-path k-dir "__squash.json"))
         (stats (make-squash-stats))
         (relset (snapshot-relpath-set config k-dir))
         (claimed (make-hash-table))
         (existing (filter (lambda (d)
                             (directory-exists? (join-path partial-dir d)))
                           sources)))
    (unless dry-run
      (call-with-output-file journal
        (lambda (p)
          (json-write-pretty (list (cons "target" target)
                                   (cons "sources" (list->vector sources)))
                             p)))
      (for-each (lambda (d)
                  (call-with-output-file
                      (join-path partial-dir d "__squashed")
                    (lambda (p) (display target p))))
                existing))
    (for-each (lambda (d)
                (merge-snapshot-into! (join-path partial-dir d)
                                      k-dir
                                      relset
                                      claimed
                                      dry-run
                                      stats))
              (list-sort string>? existing))
    (unless dry-run
      (merge-snapshot-logs! partial-dir target sources)
      (when (= (stat-ref stats 'errors) 0)
        (guard (e (#t #f)) (delete-file journal))))
    stats))

;; finish any job a previous run left behind (a __squash.json on a target).
(define (resume-pending-squash! config)
  (let ((partial-dir (norm-path (config-dest-partial config))))
    (for-each (lambda (target)
                (let
                    ((journal (join-path partial-dir target "__squash.json")))
                  (when (file-exists? journal)
                    (let
                        ((sources (vector->list
                                   (aget (read-json-file journal)
                                         "sources"))))
                      (prn (string-append "resuming interrupted squash into "
                                          target))
                      (run-squash-job! config target sources #f)))))
              (list-sort string<? (dated-snapshot-names partial-dir)))))

(define (pending-squash? partial-dir)
  (any (lambda (d) (file-exists? (join-path partial-dir d "__squash.json")))
       (dated-snapshot-names partial-dir)))

;; group snapshots (dates, any order) into jobs ((target . sources) ...):
;; every snapshot not in `kept` is merged into the next kept one after it.
;; Snapshots after the last kept one are left alone.
(define (plan-squash-jobs dates kept)
  (let loop ((ds (list-sort string<? dates))
             (pending '())
             (jobs '()))
    (cond ((null? ds) (reverse jobs))
          ((member (car ds) kept)
           (loop (cdr ds)
                 '()
                 (if (null? pending)
                     jobs
                     (cons (cons (car ds) (reverse pending)) jobs))))
          (else (loop (cdr ds) (cons (car ds) pending) jobs)))))

(define (jobs-sources jobs) (append-map cdr jobs))

;; ======================================================================
;; squash command: merge every snapshot in [from, to] into the newest
;; complete snapshot in that range.
;; ======================================================================

(define (do-squash config from to force json-out)
  (let* ((partial-dir (norm-path (config-dest-partial config)))
         (_ (when force (resume-pending-squash! config)))
         (in-range (filter (lambda (d)
                             (and (string>=? d from) (string<=? d to)))
                           (dated-snapshot-names partial-dir)))
         (target (find (lambda (d)
                         (snapshot-complete? (join-path partial-dir d)))
                       in-range))
         (sources (if target
                      (list-sort string<?
                                 (filter (lambda (d) (string<? d target))
                                         in-range))
                      '()))
         (stats (if (pair? sources)
                    (run-squash-job! config target sources (not force))
                    (make-squash-stats))))
    (if json-out
        (begin
          (json-write-pretty (list (cons "target" (or target 'null))
                                   (cons "sources" (list->vector sources))
                                   (cons "moved" (stat-ref stats 'moved))
                                   (cons "dropped" (stat-ref stats 'dropped))
                                   (cons "bytes_freed"
                                         (stat-ref stats 'bytes-freed))
                                   (cons "error_count"
                                         (stat-ref stats 'errors))
                                   (cons "force" force))
                             (current-output-port))
          (newline))
        (cond ((null? sources)
               (prn (string-append "nothing to squash in " from " .. " to)))
              (else (unless force (prn "DRY RUN (no --force)."))
                    (prn (string-append (if force "merged " "would merge ")
                                        (number->string (length sources))
                                        " snapshot(s) into "
                                        target
                                        ":"))
                    (for-each (lambda (d) (prn "  " d)) sources)
                    (print-squash-summary stats force))))
    (when (> (stat-ref stats 'errors) 0) (exit 1))))

(define (print-squash-summary stats force)
  (prn (string-append (if force "moved " "would move ")
                      (group-digits (stat-ref stats 'moved))
                      " file(s) forward, "
                      (if force "dropped " "drop ")
                      (group-digits (stat-ref stats 'dropped))
                      " superseded/deleted file(s), "
                      (if force "freed " "freeing ")
                      (format-size (stat-ref stats 'bytes-freed))))
  (when (> (stat-ref stats 'errors) 0)
    (prn (string-append "ERR: "
                        (number->string (stat-ref stats 'errors))
                        " error(s); rerun to finish the squash"))))

;; ======================================================================
;; prune: retention policy, then merge-forward of everything not kept
;; ======================================================================

(define (date->seconds s) (parse-iso8601 s))

;; dates (from snaps, newest first) kept by the policy. Only complete
;; snapshots can be kept (they are the restore points); incomplete ones are
;; folded into the next kept snapshot. A count of 'all means unlimited.
;; Always kept: today's snapshot and the newest complete snapshot.
(define
    (select-kept-snapshots snaps keep-last keep-days keep-monthly keep-yearly
                           today)
  (let* ((today-sec (date->seconds today))
         (cutoff-sec (and keep-days
                          (not (eq? keep-days 'all))
                          (- today-sec (* keep-days 86400))))
         (complete (filter (lambda (s)
                             (snapshot-complete? (snap-field s "path")))
                           snaps))
         (months (make-hash-table))
         (years (make-hash-table)))
    (define (within? n count) (or (eq? n 'all) (and n (<= count n))))
    ;; first (newest) snapshot of a bucket counts if the bucket is among the
    ;; n most recent buckets.
    (define (bucket-keep? table key n)
      (and n
           (not (hash-table-exists? table key))
           (begin
             (hash-table-set! table key #t)
             (within? n (hash-table-size table)))))
    (let loop ((ss complete) (i 0) (acc '()))
      (if (null? ss)
          (reverse acc)
          (let* ((date (snap-field (car ss) "date"))
                 (sec (date->seconds date))
                 (m (bucket-keep? months (substring date 0 7) keep-monthly))
                 (y (bucket-keep? years (substring date 0 4) keep-yearly))
                 (keep (or (string=? date today)
                           (= i 0)
                           (and keep-last (within? keep-last (+ i 1)))
                           (eq? keep-days 'all)
                           (and cutoff-sec sec (>= sec cutoff-sec))
                           m
                           y)))
            (loop (cdr ss) (+ i 1) (if keep (cons date acc) acc)))))))

(define
    (do-prune config keep-last keep-days keep-monthly keep-yearly force
              json-out)
  (let ((partial (norm-path (config-dest-partial config))))
    (when force (resume-pending-squash! config))
    (when (and (not force) (not json-out) (pending-squash? partial))
      (prn "NOTE: an interrupted squash is pending; --force finishes it"))
    (let* ((snaps (enumerate-snapshots partial))
           (dates (map (lambda (s) (snap-field s "date")) snaps))
           (kept-dates (select-kept-snapshots snaps keep-last keep-days
                                              keep-monthly
                                              keep-yearly
                                              (today-str)))
           (jobs (plan-squash-jobs dates kept-dates))
           (del-dates (list-sort string>? (jobs-sources jobs)))
           (kept (filter (lambda (d) (not (member d del-dates))) dates))
           (into (append-map (lambda (j)
                               (map (lambda (d) (cons d (car j))) (cdr j)))
                             jobs))
           (stats (make-squash-stats)))
      (define (snap-of d)
        (find (lambda (s) (string=? (snap-field s "date") d)) snaps))
      (when force
        (for-each (lambda (j)
                    (let ((st (run-squash-job! config (car j) (cdr j) #f)))
                      (for-each (lambda (k)
                                  (stat-add! stats k (stat-ref st k)))
                                '(moved dropped bytes-freed errors))
                      (unless json-out
                        (for-each (lambda (d)
                                    (unless
                                        (directory-exists?
                                         (join-path partial d))
                                      (prn
                                       (string-append "deleted " d
                                                      " (merged into "
                                                      (car j)
                                                      ")"))))
                                  (cdr j)))))
                  jobs))
      (let
          ((deleted (filter (lambda (d)
                              (not (directory-exists? (join-path partial d))))
                            del-dates)))
        (cond (json-out (json-write-pretty
                         (list (cons "kept" (list->vector kept))
                               (cons "deleted"
                                     (list->vector (if force deleted '())))
                               (cons "would_delete" (list->vector del-dates))
                               (cons "squashed_into"
                                     (list-sort
                                      (lambda (a b)
                                        (string>? (car a) (car b)))
                                      into))
                               (cons "bytes_freed"
                                     (stat-ref stats 'bytes-freed))
                               (cons "force" force))
                         (current-output-port))
                        (newline))
              (force (when (pair? jobs) (print-squash-summary stats #t)))
              (else (prn (string-append "DRY RUN (no --force). Would delete "
                                        (number->string (length del-dates))
                                        " snapshot(s), merging what later"
                                        " snapshots still need into them:"))
                    (for-each (lambda (d)
                                (let ((s (snap-of d)))
                                  (prn (string-append
                                        "  "
                                        d
                                        "  "
                                        (group-digits
                                         (snap-field s "file_count"))
                                        " files  "
                                        (format-size
                                         (snap-field s "total_bytes"))
                                        "  -> "
                                        (cdr (assoc d into))))))
                              del-dates)
                    (prn (string-append "Would keep "
                                        (number->string (length kept))
                                        " snapshot(s).")))))
      (when (> (stat-ref stats 'errors) 0) (exit 1)))))

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
    (for-each (lambda (sourcedir)
                (let* ((prefix (directory-name sourcedir))
                       (prefixlen (compute-prefixlen prefix))
                       (destdir (join-path
                                 dest-full
                                 (substring sourcedir prefixlen
                                            (string-length sourcedir)))))
                  (when (directory-exists? destdir)
                    (let ((destdir-prefixlen (compute-prefixlen destdir)))
                      (walk-files destdir
                                  (lambda (p) #f)
                                  (lambda (filepath)
                                    (let
                                        ((srcpath
                                          (join-path
                                           sourcedir
                                           (substring
                                            filepath
                                            destdir-prefixlen
                                            (string-length filepath)))))
                                      (hash-table-set!
                                       new-state
                                       srcpath
                                       (vector
                                        (file-size filepath)
                                        (quotient
                                         (file-modification-timestamp
                                          filepath)
                                         1000))))))))))
              source-dirs)
    (write-state-table (config-full-state-file config) new-state)
    (when (directory-exists? dest-partial)
      (guard (e (#t #f))
        (copy-file (config-full-state-file config)
                   (join-path dest-partial "__state.json"))))
    (prn "done")))

;; ======================================================================
;; CLI
;; ======================================================================

(define (cmd-backup config rest)
  (let loop ((args rest) (dry #f) (quiet #f) (json #f))
    (if (null? args)
        (with-lock config (lambda () (make-backup config dry quiet json)))
        (let ((a (car args)))
          (cond ((string=? a "--dry-run") (loop (cdr args) #t quiet json))
                ((or (string=? a "--quiet") (string=? a "-q"))
                 (loop (cdr args) dry #t json))
                ((string=? a "--json") (loop (cdr args) dry quiet #t))
                (else (eprint "backup: unknown argument " a) (exit 2)))))))

(define (cmd-restore config rest)
  (let loop ((args rest) (dest #f) (ts #f) (dry #f) (force #f) (pos '()))
    (if (null? args)
        (let* ((patterns (reverse pos))
               (date-first (and (not ts)
                                (pair? patterns)
                                (valid-date-string? (car patterns))))
               (ts2 (if date-first (car patterns) ts))
               (patterns2 (if date-first (cdr patterns) patterns)))
          (if (not dest)
              (begin
                (eprint "restore: dest_dir is required")
                (exit 2))
              (restore config dest (or ts2 (today-str)) patterns2 dry force)))
        (let ((a (car args)))
          (cond ((or (string=? a "-t") (string=? a "--timestamp"))
                 (if (null? (cdr args))
                     (begin
                       (eprint "restore: " a " needs a value")
                       (exit 2))
                     (loop (cddr args) dest (cadr args) dry force pos)))
                ((string=? a "--dry-run")
                 (loop (cdr args) dest ts #t force pos))
                ((string=? a "--force") (loop (cdr args) dest ts dry #t pos))
                ((string-prefix? "--" a)
                 (eprint "restore: unknown option " a)
                 (exit 2))
                ((not dest) (loop (cdr args) a ts dry force pos))
                (else (loop (cdr args) dest ts dry force (cons a pos))))))))

(define (cmd-package config rest)
  (let loop ((args rest) (pos '()) (full #f) (force #f))
    (if (null? args)
        (let ((positional (reverse pos)))
          (if (< (length positional) 2)
              (begin
                (eprint "package: dest_dir and max_size are required")
                (exit 2))
              (let ((destdir (list-ref positional 0))
                    (max-size (parse-size (list-ref positional 1)))
                    (timestamp (if (>= (length positional) 3)
                                   (list-ref positional 2)
                                   (today-str))))
                ;; dest-exists check happens BEFORE acquiring the lock: a thunk
                ;; that calls (exit) under with-lock would leak the lock file.
                (if (and (path-exists? destdir) (not force))
                    (begin
                      (prn "package-data")
                      (prn (string-append "ERR: " destdir " exists, abort"))
                      (exit 1))
                    (with-lock config
                      (lambda ()
                        (package-data config destdir max-size timestamp full
                                      force)))))))
        (let ((a (car args)))
          (cond ((string=? a "--full") (loop (cdr args) pos #t force))
                ((string=? a "--force") (loop (cdr args) pos full #t))
                ((string-prefix? "--" a)
                 (eprint "package: unknown option " a)
                 (exit 2))
                (else (loop (cdr args) (cons a pos) full force)))))))

;; a retention count: a non-negative integer, or "all" (unlimited).
(define (parse-count cmd opt str allow-all)
  (cond ((and allow-all (string=? str "all")) 'all)
        ((let ((n (string->number str)))
           (and n (exact-integer? n) (>= n 0) n)))
        (else (eprint cmd ": " opt " needs a non-negative integer"
                      (if allow-all " or 'all'" "")
                      ", got "
                      str)
              (exit 2))))

(define (cmd-prune config rest)
  (let loop ((args rest) (opts '()) (force #f) (json #f))
    (define (opt k) (let ((p (assq k opts))) (and p (cdr p))))
    (if (null? args)
        (if (null? opts)
            (begin
              (eprint "prune: must specify --keep-last, --keep-days,"
                      " --keep-monthly and/or --keep-yearly")
              (exit 2))
            (let
                ((run (lambda (f)
                        (do-prune config
                                  (opt 'last)
                                  (opt 'days)
                                  (opt 'monthly)
                                  (opt 'yearly)
                                  f
                                  json))))
              (if force (with-lock config (lambda () (run #t))) (run #f))))
        (let* ((a (car args))
               (key (cond ((string=? a "--keep-last") 'last)
                          ((string=? a "--keep-days") 'days)
                          ((string=? a "--keep-monthly") 'monthly)
                          ((string=? a "--keep-yearly") 'yearly)
                          (else #f))))
          (cond (key (when (null? (cdr args))
                       (eprint "prune: " a " needs a value")
                       (exit 2))
                     (loop (cddr args)
                           (cons (cons key
                                       (parse-count
                                        "prune"
                                        a
                                        (cadr args)
                                        (memq key '(monthly yearly))))
                                 opts)
                           force
                           json))
                ((string=? a "--force") (loop (cdr args) opts #t json))
                ((string=? a "--json") (loop (cdr args) opts force #t))
                (else (eprint "prune: unknown argument " a) (exit 2)))))))

(define (cmd-squash config rest)
  (let loop ((args rest) (from #f) (to #f) (force #f) (json #f))
    (if (null? args)
        (if (not (and from to))
            (begin
              (eprint "squash: --from YYYY-MM-DD and --to YYYY-MM-DD are"
                      " required")
              (exit 2))
            (if force
                (with-lock config
                  (lambda () (do-squash config from to #t json)))
                (do-squash config from to #f json)))
        (let ((a (car args)))
          (cond ((or (string=? a "--from") (string=? a "--to"))
                 (when
                     (or (null? (cdr args))
                         (not (valid-date-string? (cadr args))))
                   (eprint "squash: " a " needs a date YYYY-MM-DD")
                   (exit 2))
                 (if (string=? a "--from")
                     (loop (cddr args) (cadr args) to force json)
                     (loop (cddr args) from (cadr args) force json)))
                ((string=? a "--force") (loop (cdr args) from to #t json))
                ((string=? a "--json") (loop (cdr args) from to force #t))
                (else (eprint "squash: unknown argument " a) (exit 2)))))))

(define (cmd-list-cmd config rest)
  (let loop ((args rest) (json #f))
    (if (null? args)
        (cmd-list config json)
        (let ((a (car args)))
          (cond ((string=? a "--json") (loop (cdr args) #t))
                (else (eprint "list: unknown argument " a) (exit 2)))))))

(define (cmd-init-cmd rest)
  (let loop ((args rest) (name "backup-config.json") (force #f))
    (if (null? args)
        (cmd-init name force)
        (let ((a (car args)))
          (cond ((string=? a "--name")
                 (if (null? (cdr args))
                     (begin
                       (eprint "init: --name needs a value")
                       (exit 2))
                     (loop (cddr args) (cadr args) force)))
                ((string=? a "--force") (loop (cdr args) name #t))
                (else (eprint "init: unknown argument " a) (exit 2)))))))

(define (usage)
  (eprint "usage: scm dabbak.scm <command> [options]")
  (eprint
   "commands: init backup restore list prune squash package refresh-state config"))

(define (main argv)
  (if (null? argv)
      (begin
        (usage)
        (exit 2))
      (let ((cmd (car argv))
            (rest (cdr argv)))
        (cond ((string=? cmd "init") (cmd-init-cmd rest))
              ((string=? cmd "gui")
               (eprint
                "gui is not available in the Scheme port; use the legacy-python branch for the Tkinter GUI")
               (exit 1))
              (else (let ((config (read-config)))
                      (cond ((string=? cmd "backup") (cmd-backup config rest))
                            ((string=? cmd "restore")
                             (cmd-restore config rest))
                            ((string=? cmd "package")
                             (cmd-package config rest))
                            ((string=? cmd "refresh-state")
                             (with-lock config
                               (lambda () (refresh-state config))))
                            ((string=? cmd "config") (cmd-config config))
                            ((string=? cmd "list") (cmd-list-cmd config rest))
                            ((string=? cmd "prune") (cmd-prune config rest))
                            ((string=? cmd "squash") (cmd-squash config rest))
                            (else (eprint "error: unknown command " cmd)
                                  (usage)
                                  (exit 2)))))))))

(main (cdr (command-line)))
