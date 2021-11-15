package entity;

import java.io.Serializable;

public class TweetSentiment implements Serializable {
    public long id;
    public String key_word;
    public int sentiment;

    public TweetSentiment(long id, String key_word, int sentiment) {
        this.id = id;
        this.key_word = key_word;
        this.sentiment = sentiment;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getKey_word() {
        return key_word;
    }

    public void setKey_word(String key_word) {
        this.key_word = key_word;
    }

    public int getSentiment() {
        return sentiment;
    }

    public void setSentiment(int sentiment) {
        this.sentiment = sentiment;
    }
}
