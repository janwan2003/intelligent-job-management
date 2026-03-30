#ifndef TIMING_HH
#define TIMING_HH

#include <chrono>
#include <ctime>
#include <iostream>

namespace timing
{
  typedef std::chrono::time_point<std::chrono::system_clock> time_point;

  time_point now (void);

  void elapsed_between (const time_point & start, const time_point & finish);
}

#endif /* TIMING_HH */