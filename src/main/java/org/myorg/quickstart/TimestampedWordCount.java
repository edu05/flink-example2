package org.myorg.quickstart;

public class TimestampedWordCount {
    private TimestampedWord timestampedWord;
    private int count;

    public TimestampedWordCount() {
    }

    public TimestampedWordCount(TimestampedWord timestampedWord, int count) {
        this.timestampedWord = timestampedWord;
        this.count = count;
    }

    public TimestampedWord getTimestampedWord() {
        return timestampedWord;
    }

    public int getCount() {
        return count;
    }

    public String getWord() {
        return timestampedWord.getWord();
    }

    @Override
    public String toString() {
        return timestampedWord.toString() + " -> " + count;
    }

    public void setTimestampedWord(TimestampedWord timestampedWord) {
        this.timestampedWord = timestampedWord;
    }

    public void setCount(int count) {
        this.count = count;
    }
}