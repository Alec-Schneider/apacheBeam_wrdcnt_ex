## Apache Beam Word Count Examples With Text Files

[wordcount.py](./wordcount.py) file reads the MLK "I Have A Dream" and Theodore Roosevelt's "Man In The Arena" speech and counts the number of times each word appears <br>

[wordcount2.py](./wordcount2.py) is a command line script that reads text files from the "--input_path" argument passed to it and write the word counts to the "--output_path" argument <br>

Command line example:

```bash
python wordcount2.py --input_path="input/*" --output_path="output/word_count.txt"
```

Scripts were written with code from https://beam.apache.org/get-started/wordcount-example/
