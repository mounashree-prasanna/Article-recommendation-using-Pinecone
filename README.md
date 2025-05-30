# 🔍 Medium Article Semantic Search with Pinecone & Airflow

This project automates the process of building a semantic search engine for Medium articles using **Apache Airflow** and **Pinecone vector database**. It downloads article data, generates embeddings using **Sentence Transformers**, and stores them in Pinecone for similarity-based retrieval.

---

## 🚀 Project Overview

The goal is to enable semantic search over Medium article titles and subtitles by leveraging vector embeddings. The end-to-end pipeline is orchestrated using **Apache Airflow**, scheduled weekly, and includes the following steps:

1. Download Medium article dataset from a public URL.
2. Preprocess and clean the article metadata.
3. Generate vector embeddings using a pre-trained MiniLM transformer model.
4. Create/reset a Pinecone index and upload the embeddings.
5. Test the setup with a semantic search query.

---

## 🛠️ Tech Stack

| Component       | Tool                                |
|----------------|-------------------------------------|
| Orchestration   | Apache Airflow                      |
| Embedding Model | SentenceTransformers (MiniLM)       |
| Vector Database | Pinecone                            |
| Language        | Python                              |
| Deployment      | Airflow DAG                         |

---
## 🧱 DAG Task Breakdown

- **`download_data`**: Downloads a Medium article dataset (CSV) to `/tmp`.
- **`preprocess_data`**: Cleans titles/subtitles, creates metadata, assigns unique IDs.
- **`create_pinecone_index`**: Initializes or resets the Pinecone index.
- **`generate_embeddings_and_upsert`**: Uses MiniLM to create embeddings and upserts to Pinecone in batches.
- **`test_search_query`**: Runs a sample semantic query against the Pinecone index and logs the top 5 matches.

---
## 🔍 Example Query Output

> **Query:** "what is ethics in AI"

Returns top 5 most semantically similar Medium articles using vector similarity from Pinecone.

---
