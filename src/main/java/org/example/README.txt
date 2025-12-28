Assigment 2
Maor Tabachnik - 324184670
Michael Vinokur - 212237028

Implementation Overview:
The project implements a scalable three-stage MapReduce pipeline to calculate the Log-Likelihood Ratio (LLR)
Step 1: Frequency Counting
Mapper: Scans the 2-gram dataset. For each valid bigram (not containing stop words), it emits four keys: the decade-total, the first word count, the second word count, and the pair count.
Combiner: Performs local aggregation to significantly reduce the data sent over the network.
Partitioner: Ensures all keys belonging to the same decade are sent to the same reducer to maintain consistency.

Step 2: LLR Calculation
Reducer: Collects the counts from Job 1. It uses an Order Inversion pattern to ensure the total decade count and individual word counts arrive before the specific pair counts.
Formula: It calculates the LLR score using Dunning’s formula.

Step 3: Sorting and Top-100
Reducer: Uses a PriorityQueue of size 100, this allows the reducer to efficiently keep only the 100 highest-scoring collocations for each decade.

Map-Reduce Statistics (Hebrew):

    Without Local Aggregation(Direct Mapper Output):
    Key-Value Pairs Produced: Map output records = 474,613,820
    Total Data Size: 15,232,870,292 bytes (approximately 14.18 GB).

    With Local Aggregation(Sent to Reducers via Combiner):
    Key-Value Pairs Sent:Combine output records = 101,023,915
    Efficiency Gain: The use of a Combiner reduced the number of key-value pairs sent over the network by approximately 78.7% (from ~474M to ~101M).

Hebrew Collocations Analysis:
Good Examples:

בראש השנה (Decade: 1670, LLR: 190.0)
This is a strong collocation as it represents a specific Jewish holiday

בית המקדש (Decade: 1850, LLR: 4820.8)
A classic historical and religious term

ראש הממשלה (Decade: 2000, LLR: 537741.1)
A modern political term. Its extremely high score indicates it is a fixed expression in news and official documents

מלחמת העולם (Decade: 1970, LLR: 199123.5)
A fixed historical term. The high LLR reflects the consistent co-occurrence of these words in modern historical contexts

דרכי החכמה (Decade: 1820, LLR: 2266.3)
This represents a common phrase in hebrew literature, fitting for the literary era

Bad Examples:
NaN (שמעון ולוי) (Decade: 1670)
This result returned "Not a Number" due to a calculation error. It often happens when the LLR formula encounters division by zero or log of zero on small data samples

9 | 9 (Decade: 1820, LLR: 1890.9)
This is "raw noise" from the Google N-grams dataset.

אנו רואים (Decade: 1910, LLR: 8363.8)
This is a grammatical structure rather than a semantic collocation, it does not represent a unique concept

כדי שלא (Decade: 1850, LLR: 6507.4)
These should ideally be filtered by an extensive stop words list

• | כלומר (Decade: 1820, LLR: 2489.3)
This is an extraction error caused by inconsistencies in the original Google N-grams text cleaning

Map-Reduce Statistics (English):

Without Local Aggregation(Direct Mapper Output):
    Key-Value Pairs Produced: Map output records = 1,186,534,550
    Total Data Size: 38.5 GB.

    With Local Aggregation(Sent to Reducers via Combiner):
    Key-Value Pairs Sent:Combine output records = 225,441,564
    Efficiency Gain: The use of a Combiner reduced the number of key-value pairs sent over the network by approximately 81% (from ~1.18B to ~225M).

Hebrew Collocations Analysis:
Good Examples:
United States (Decade: 1970, LLR: 482,854.2)
The highest-ranking pair, representing a single geographical and political entity.

New York (Decade: 1940, LLR: 312,410.5)
A classic proper noun collocation where the words appear together with extreme regularity.

Human Rights (Decade: 1990, LLR: 185,220.8)
A fixed socio-political term where the combination carries a specific definition.

Social Security (Decade: 1980, LLR: 164,050.4)
An institutional term illustrating the algorithm's ability to identify formal compound nouns.

World War (Decade: 1950, LLR: 142,330.1)
A historical term that remains highly significant across multiple decades in the dataset.

Bad Examples:
of the (Decade: 1950, LLR: 8540.2)
A classic example of function word noise, they do not form a semantic collocation.

"19 45" (Decade: 1960, LLR: 5210.8)
Numerical noise common in historical texts.

"said he" (Decade: 1890, LLR: 4890.5)
A grammatical narrative pattern. While statistically frequent in literature, it lacks the semantic "bond" of a true collocation.

"et al" (Decade: 1990, LLR: 3950.1)
Academic citation noise.

"p ." (Decade: 1920, LLR: 3120.4)
A tokenization artifact. This usually represents page citations (e.g., "p. 12") which the algorithm captures due to the punctuation not being fully filtered.

