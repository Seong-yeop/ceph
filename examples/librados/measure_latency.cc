#include "rados_io.hpp"

#include <rados/librados.hpp>
#include <iostream>
#include <string>
#include <fstream>

#define IO_SIZE (4*(1024)*(1024))

int main(int argc, const char **argv){

  std::ofstream flrtime;
  std::ofstream frltime;
  size_t len = 0;

  rados_io::conn_info ci = {
    .user = "client.admin",
    .cluster = "ceph",
    .flags = 0,
  };

  rados_io rio(ci, "data4");
  
  std::map<std::string, clock_t> lrtime;
  std::map<std::string, clock_t> rltime;

  rio.prepare_bench(IO_SIZE);

  //len = rio.write_bench(100, lrtime, rltime, IO_SIZE); 
  len = rio.read_bench(100, lrtime, rltime, IO_SIZE); 
  std::cout << "Total Write: " << len << '\n';
  
  rio.close_bench();

  std::cout << "bench completed" << '\n';

  flrtime.open("lrtime.txt");
  if (flrtime.is_open()) {
    for (auto it = lrtime.begin(); it != lrtime.end(); it++) {
      auto key = it->first.c_str();
      auto value = std::to_string(it->second).c_str();
      flrtime.write(key ,sizeof(key));
      flrtime.write(value, sizeof(value));
      flrtime << '\n';
    }
  }

  frltime.open("rltime.txt");
  if (frltime.is_open()) {
    for (auto it = rltime.begin(); it != rltime.end(); it++) {
      auto key = it->first.c_str();
      auto value = std::to_string(it->second).c_str();
      frltime.write(key ,sizeof(key));
      frltime.write(value, sizeof(value));
      frltime << '\n';
    }
  }

  flrtime.close();
  frltime.close();



  return 0;
}
