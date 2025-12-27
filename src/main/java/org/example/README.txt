Assigment 2
Maor Tabachnik - 324184670
Michael Vinokur - 212237028

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