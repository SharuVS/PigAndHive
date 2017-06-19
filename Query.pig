A = LOAD 'Query1.csv' USING PigStorage(',') AS (Id:int, Score:int);
DUMP A;
B = ORDER A BY score DESC;
C = LIMIT B 11;
D = FOREACH C GENERATE Id, Score;
DUMP D;
A = LOAD ‘Query2.csv’ USING PigStorage(‘,’) AS (OwnerUserId:int , Score:int);
B = GROUP A BY OwnerUserId;
C = FOREACH B GENERATE group, SUM(A.Score) AS TOTALSCORE;
D = ORDER C BY TOTALSCORE DESC;
E = LIMIT D 11;
DUMP E;
A = LOAD ‘Query3.csv’ USING PigStorage(‘,’) AS (OwnerUserId:int , Body:chararray);
B = FOREACH A GENERATE FLATTEN(TOKENIZE((chararray)$1)) AS WORD, OwnerUserId;
C = FILTER B BY WORD MATCHES '\\hadoop+' OR WORD MATCHES '\\Hadoop+' OR WORD MATCHES '\\ApacheHadoop+' OR WORD MATCHES '\\HADOOP+' OR WORD MATCHES '\\APACHEHADOOP+' OR WORD MATCHES ‘\\apache hadoop+’ OR WORD MATCHES ‘\\Hadoop Apache+’;
D = group C by OwnerUserId;
E = foreach D generate COUNT(C), C.WORD, group;
DUMP E;
DEFINE body_post (--id0 , --body1) SHIP('Query.csv')
raw_documents = LOAD '$DOCS' AS (post_id:chararray, Body:chararray);
termized     = STREAM raw_documents THROUGH body_post AS (post_id:chararray, term:chararray);
doc_terms       = GROUP termized BY (post_id, term);
doc_term_counts = FOREACH doc_terms GENERATE FLATTEN(group) AS (post_id, term), COUNT(termized) AS terms_per_post;
doc_usage_bag    = GROUP doc_term_counts BY post_id;
doc_usage_bag_fg = FOREACH doc_usage_bag GENERATE
                    group                                                 AS post_id,
                    FLATTEN(doc_term_counts.(term, terms_per_post)) AS (term, terms_per_post), 
                    SUM(doc_term_counts.terms_per_post)              AS doc_size
                  ;
term_freqs = FOREACH doc_usage_bag_fg GENERATE
              post_id                                          AS post_id,
              term                                           AS term,
              ((double)terms_per_post / (double)doc_size) AS term_freq;
            ;
             
term_usage_bag  = GROUP term_freqs BY term;
term_usages    = FOREACH term_usage_bag GENERATE
                   FLATTEN(term_freqs) AS (post_id, term, term_freq),
                   COUNT(term_freqs)   AS num_post_with_term ;
tfidf_all = FOREACH term_usages {
             idf    = LOG((double)$NDOCS/(double)num_post_with_term);
             tf_idf = (double)term_freq*idf;
               GENERATE
                 post_id AS post_id,
                 term  AS term,
                 tf_idf AS tf_idf ;
            };
STORE tfidf_all INTO '$OUT';