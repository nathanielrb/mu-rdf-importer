(use s-sparql)

(use csv csv-string srfi-127 matchable)

(define parse (csv-parser #\,))

(define (lazy-data path)
  (let ((port (open-input-file path)))
    (generator->lseq
     (lambda ()
       (let ((line (read-line port)))
	 (if (eof-object? line)
	     (close-input-pipe port)
	     (csv-record->list
	      (join
	       (parse line)))))))))

(define (load-csv path proc #!optional (lines #f))
  (let loop ((data (lazy-data path))
	     (n 0)
	     (accum '()))
    (if (and data
	     (lseq-car data)
	     (or (not lines)
		 (< n lines)))
	(begin (proc (lseq-car data))
	(loop (lseq-cdr data)
	      (+ n 1)
	      accum) )
	accum)))

(define-syntax triples-template
  (syntax-rules ()
    ((triples-template (vars ...) body)
     (match-lambda
       ((vars ...)
        body)))))

(define sparql/update print)

(define (load-dataset path template #!optional (lines #f))
  (load-csv path
            (compose sparql/update s-insert s-triples template)
            lines))

(define (uuid label)
  (->string (gensym label)))

(define (%uri template #!rest args)
  (read-uri (apply format #f template args)))

(define (%str template #!rest args)
  (apply format #f template args))

(define template
  (triples-template
   (supermarket ecoicop isba isba-desc
                esba esba-desc gtin gtin-desc quantity unit)
   (let* ((id (uuid "observation"))
         (uri (%uri "http://data.europa.eu/eurostat/data/id/observation/~A" id)))
     `((,uri mu:uuid ,id)))))

;; (define training-triples
;;   (match-lambda
;;    ((supermarket ecoicop ecoicop6 ecoicop6-desc
;; 		 esba esba-desc gtin gtin-desc quantity unit)
;;     (let ((p (obs (uuid "observation")))
