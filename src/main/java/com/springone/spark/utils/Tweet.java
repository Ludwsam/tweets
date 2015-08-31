package com.springone.spark.utils;

import java.io.Serializable;

public class Tweet implements Serializable {

  long id;
  String name;
  String text;
  String lang;

  public Tweet() {
  }

  public Tweet( String lang, long id, String name, String text) {
    this.lang = lang;
    this.id = id;
    this.name = name;
    this.text = text;
  }

  public String getLang() {return lang; }

  public long getId() {
    return id;
  }

  public String getName() { return name;}

  public String getText() { return text;}

  @Override
  public String toString(){
    return getLang() + ", " + getId() + ", " + getName() + ", " + getText();
  }
}
