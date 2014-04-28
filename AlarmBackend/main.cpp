#define ALARM_READ_QUEUE_SIZE (1 << 22)
#define ALARM_TABLE_WRITE_SIZE (1 << 22)
#define BASE_DIR "W:\\"
#define SQL_CONNECT_STR "odbc:DSN=GFUNC"

#include "../LocalConnector/LocalConnector/connector.h"
#include "../LocalConnector/LocalConnector/shared_memory_file/shared_memory_file.h"
#include "../LocalConnector/LocalConnector/queue/shared_queue.h"

using namespace tsoft;
using namespace queue;

#include "objects.h"

#include <vector>
#include <iostream>
#include <sstream>
#include <windows.h>

#include "../cppdb/cppdb/frontend.h"

void main()
{
  typedef tsoft::shared_memory_file<ALARM_READ_QUEUE_SIZE, true> read_transport_type;
  typedef tsoft::shared_memory_file<ALARM_TABLE_WRITE_SIZE, true> write_transport_type;

  typedef queue::shared_queue<ALARM_READ_QUEUE_SIZE> read_queue;
  typedef queue::shared_queue<ALARM_TABLE_WRITE_SIZE> write_queue;

  typedef tsoft::connector<read_transport_type, read_queue> read_connector;
  typedef tsoft::connector<write_transport_type, write_queue> write_connector;

  read_connector &read = *read_connector::ConstructContainer(new read_transport_type(BASE_DIR "read_alarms.mem"), ALARM_READ_QUEUE_SIZE);
  write_connector &write = *write_connector::ConstructContainer(new write_transport_type(BASE_DIR "table_alarms.mem"), ALARM_READ_QUEUE_SIZE);

  int read_count = 0;

  while (true)
  {
    shared_alarm sh;
    bool readed = false;

    if (read->IsEmpty())
    {
      Sleep(100);
      continue;
    }
    try
    {
      sh = read->Pop<shared_alarm>();
      readed = true;
    } catch (not_ready &)
    {
    } catch (queue::empty &)
    {
    } catch (pop_fault &)
    {
      // FATAL
    } catch (shared_queue_fault &)
    {
      // FATAL
    }

    if (!readed)
      continue;
    cppdb::session con(SQL_CONNECT_STR);
    int id_alarm;
    cppdb::result res = con 
      << "SELECT ID FROM dbo.ALARM WHERE TYPE like ? AND ID_TAG=(SELECT ID FROM dbo.TAG WHERE NAME like ?)" 
      << (std::string)sh.type
      << (std::string)sh.tag_name
      << cppdb::row;

    if(res.empty())
    {
      std::cout 
        << "Error finding alarm id "
        << (std::string)sh.type
        << (std::string)sh.tag_name << std::endl;
      continue;
    }

    id_alarm = res.get<int>("ID");

    cppdb::statement stat = con
      << "INSERT INTO dbo.ACTIVE(ID_ALARM, STATE, ACK, IS_NR, DYN_PRIORITY, BLK_PRIORITY) VALUES (?, 1, 1, 1, 1, 1)"
      << id_alarm;

    stat.exec();
    read_count++;

    if (!read->IsEmpty() || (read_count % 30) != 0)
      continue;

    // Update screens only if all ready

    cppdb::result table = con 
      << "SELECT dbo.ALARM.TYPE AS TYPE, dbo.TAG.NAME AS NAME, dbo.ACTIVE.STATE, dbo.ACTIVE.ACK, dbo.ACTIVE.IS_NR "
      "FROM dbo.ACTIVE "
      "JOIN dbo.ALARM ON dbo.ACTIVE.ID_ALARM=dbo.ALARM.ID "
      "JOIN dbo.TAG ON dbo.ALARM.ID_TAG=dbo.TAG.ID "      
      "ORDER BY DYN_PRIORITY, BLK_PRIORITY, TIME DESC ";

    std::vector<shared_table_row> rows;

    while (table.next())
    {
      shared_table_row t;
      std::string tmp;
      table >> tmp;
      strcpy(t.sh.type, tmp.c_str());
      table >> tmp;
      strcpy(t.sh.tag_name, tmp.c_str());
      rows.push_back(t);
    }

    std::vector<char> mem(sizeof(shared_alarm_table) + sizeof(char[STRING_LENGTH]) * (rows.size() - 1));
    shared_alarm_table *ptr = new (&mem[0])shared_alarm_table;
    ptr->count = rows.size();
    for (int i = 0; i < rows.size(); i++)
    {
      std::stringstream ss;
      ss << rows[i].sh.tag_name << " " << rows[i].sh.type;
      strcpy(ptr->Get(i), ss.str().c_str());
    }

    write->Push((unsigned char *)&mem[0], mem.size());
    ptr->~shared_alarm_table();
  }
}