#pragma once

#define STRING_LENGTH 30

struct shared_alarm
{
  char tag_name[STRING_LENGTH], type[STRING_LENGTH];
};

struct shared_table_row
{
  shared_alarm sh;
};

struct shared_alarm_table
{
  int count;
private:
  char first[STRING_LENGTH];
public:
  char *Get( int i ) const
  {
    char *ptr = (char *)(first);
    return ptr + i * sizeof(first);
  }
};