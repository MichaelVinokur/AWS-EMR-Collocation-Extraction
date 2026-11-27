# Collocation Extraction with Hadoop MapReduce on AWS EMR

A large-scale **Hadoop MapReduce** pipeline in **Java** that finds **collocations** (word pairs that appear together more often than chance, like *"strong tea"*) in the **Google Books N-grams** dataset, decade by decade. It runs on an **AWS EMR** cluster.

**Tech:** Java · Hadoop MapReduce · AWS EMR · S3 · AWS SDK for Java · Maven

---

## Pipeline (3 chained MapReduce jobs)

| Job | What it does |
|---|---|
| **1. Counts** | Reads the 2-gram dataset, removes stop words and counts per decade: `c(w1,w2)`, `c(w1)`, `c(w2)` and `N`. |
| **2. LLR** | Computes the **Log-Likelihood Ratio** score of each word pair within its decade. |
| **3. Top-100** | Keeps the 100 strongest collocations per decade. |

## Optimizations
- **Combiner** in Job 1 does local aggregation, which greatly reduces the data sent over the network in the shuffle.
- **Custom Partitioner** (`DecadePartitioner`) routes all records of the same decade to the same reducer.
- **Order inversion** with a custom composite key (`KeywordKey`, a `WritableComparable`): the sort order makes the total `N` and the single-word counts reach the reducer before the pair counts. This lets the LLR be computed in one streaming pass.
- Stop words are loaded from S3 into each mapper during setup.

## Running on EMR
`App.java` launches the cluster with the AWS SDK: 5 × `m4.large` instances, the JAR and stop-word list taken from S3, and logs written to S3.

```bash
mvn clean package
# upload the JAR and the stop-words file to your S3 bucket, then:
export AWS_ACCESS_KEY_ID=...  AWS_SECRET_ACCESS_KEY=...  AWS_SESSION_TOKEN=...
java -cp target/<jar-name>.jar org.example.App
```
Supports both **English** and **Hebrew** corpora. Choose the input and stop-word paths in `App.java`.

> Credentials are read from environment variables only. Nothing is stored in code.

---
*Team project – Distributed System Programming course, Ben-Gurion University.*
