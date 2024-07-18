Introduction:
This repository contains implementation of a Search Engine that has been implemented through Map Reduce.

Implementatoin:
Apache Hadoop has bene used to handle the distribution of input data blocks, striving to minimize data movement across the network by processing data locally whenever feasible. Hadoop's resource management capabilities dynamically allocate computational resources to tasks, optimizing cluster utilization and performance. Overall, Hadoop serves as the underlying framework that enables the scalable and parallel processing of data in the MapReduce model, making it feasible to tackle large-scale data analysis tasks efficiently.

Vector Space Model for Information Retrieval:
To understand the vector space model better, we have created a  small
collection of three documents:
 1) Term:
 	we have made a  term refers to a distinct word.
 2) Vocabulary:
 	We made  vocabulary consists of all the unique terms found in the corpus.
 3) Term Frequency :
	in Term Frequency (TF) we have represented how often a term t appears in a document d.
	The documents mentioned earlier can be depicted using Term Frequency (TF), where
	each term is formatted as (ID, frequency).

Inverse Document Frequency (IDF):
Inverse Document Frequency (IDF) indicates the number of documents in which
a term appears, reflecting its commonality. A high Inverse Document Frequency (IDF)
suggests that the term is not particularly distinctive across documents.

TF/IDF Weights:
TF/IDF weights are essentially term frequencies adjusted by Inverse Document
Frequency (IDF) normalisation. In this step we have observed the terms with IDs and reduce them in scale.

Basic Vector Space Model:
In the basic vector space model, documents and queries are represented as
vectors, reflecting their TF/IDF weights.
we have follow the these steps:
    Iterate through term frequencies in each document.
    Calculate TF by dividing term frequency by total words in the document.
    Retrieve IDF from the idf_values dictionary.
    Compute TF-IDF as the product of TF and IDF.
    Store TF-IDF weights for each term under the corresponding document ID in tfidf_weights dictionary.

