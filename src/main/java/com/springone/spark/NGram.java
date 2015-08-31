package com.springone.spark;


import java.util.ArrayList;
import java.util.List;

/**
 * n-gram implementation
 * http://stackoverflow.com/questions/3656762/n-gram-generation-from-a-sentence
 */
public class NGram {

  public static Iterable<String> ngramsword(int n, String str) {
    List<String> ngrams = new ArrayList<>();
    String[] words = str.split(" ");

    for (int i = 0; i < words.length - n + 1; i++) {
      //ngrams.add(concat(words, i, i + n));
    }

    return ngrams;

  }

  public static Iterable<String> ngrams(int n, String str) {
    List<String> ngrams = new ArrayList<>();
    char[] words = str.toCharArray();

    for (int i = 0; i < words.length - n + 1; i++) {
      ngrams.add(concat(words, i, i + n));
    }

    return ngrams;

  }
  public static String concat(char[] words, int start, int end) {
    StringBuilder sb = new StringBuilder();

    for (int i = start; i < end; i++) {
      sb.append((i > start ? " " : "") + words[i]);
    }

    return sb.toString();
  }

  /*public static String concat(String[] words, int start, int end) {
    StringBuilder sb = new StringBuilder();

    for (int i = start; i < end; i++) {
      sb.append((i > start ? " " : "") + words[i]);
    }

    return sb.toString();
  }*/

  public static void main(String[] args) {

    for (int n = 1; n < 4; n++) {
      for (String ngram : ngrams(n, "This is my car.")) {
        System.out.println(ngram);
      }

      System.out.println();
    }

  }
}
