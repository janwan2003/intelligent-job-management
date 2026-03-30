import requests
import sys


def main (port, req_id):
  # url of the request
  url = "http://0.0.0.0:" + str(port) + "/optimizer/v5"

  # first request
  if (req_id == 1):
    # data
    sample_data = {"jobs": {"JJ1": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Fri 24 Jul 2020, 07.16.50",
                      "Priority":          1,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 217.7386529,
                                                   "2": 226.5392846,
                                                   "3": 218.7260749,
                                                   "4": 206.7594049}
                          }},
              "JJ2": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 13.37.01",
                      "Priority":          2,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 73.83799561,
                                                   "2": 77.89974382999999,
                                                   "3": 76.45150798,
                                                   "4": 73.62577612000001}
                          }},
              "JJ3": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 10.59.50",
                      "Priority":          3,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 73.83799561,
                                                   "2": 77.89974382999999,
                                                   "3": 76.45150798,
                                                   "4": 73.62577612000001}
                          }},
              "JJ4": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 19.34.15",
                      "Priority":          4,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 75.48717436,
                                                   "2": 81.05718885,
                                                   "3": 81.15076952,
                                                   "4": 79.87524173}
                          }}
        },
     "nodes": {"n1": {"GPUtype":      "M60", 
                      "free_nGPUs":   4, 
                      "total_nGPUs":  4, 
                      "cost":         0.0},
               "n2": {"GPUtype":      "M60", 
                      "free_nGPUs":   4, 
                      "total_nGPUs":  4, 
                      "cost":         0.0}},
     "GPUcosts": {"K80": {"1": 0.56,
                          "2": 1.13,
                          "3": 1.69,
                          "4": 2.25,
                          "5": 2.8075,
                          "6": 3.3650,
                          "7": 3.9225,
                          "8": 4.48},
                  "M60": {"1": 0.62,
                          "2": 1.24,
                          "3": 1.86,
                          "4": 2.48,
                          "5": 3.10,
                          "6": 3.72,
                          "7": 4.34,
                          "8": 4.96}
        },
     "currentTime": "Tue 21 Jul 2020, 17.56.59",
     "verbose": 2
     }

    # expected result
    expected_result = {"estimated_cost": 50.866, 
                       "estimated_rescheduling_time": "Wed 22 Jul 2020, 14.27.37", 
                       "jobs": {"JJ1": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n1"}, 
                                "JJ2": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n1"}, 
                                "JJ3": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n1"}, 
                                "JJ4": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n1"}
                                }
                      }

  # second request
  elif (req_id == 2):
    # data
    sample_data = {"jobs": {"JJ1": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Fri 24 Jul 2020, 07.16.50",
                      "Priority":          1,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 217.7386529,
                                                   "2": 226.5392846,
                                                   "3": 218.7260749,
                                                   "4": 206.7594049}
                          }},
              "JJ2": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 13.37.01",
                      "Priority":          5,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 73.83799561,
                                                   "2": 77.89974382999999,
                                                   "3": 76.45150798,
                                                   "4": 73.62577612000001}
                          }},
              "JJ3": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 10.59.50",
                      "Priority":          3,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 73.83799561,
                                                   "2": 77.89974382999999,
                                                   "3": 76.45150798,
                                                   "4": 73.62577612000001}
                          }},
              "JJ4": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 19.34.15",
                      "Priority":          2,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 75.48717436,
                                                   "2": 81.05718885,
                                                   "3": 81.15076952,
                                                   "4": 79.87524173}
                          }},
              "JJ5": {"SubmissionTime":    "Tue 21 Jul 2020, 18.04.01",
                      "Deadline":          "Fri 24 Jul 2020, 04.37.12",
                      "Priority":          1,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 217.7386529,
                                                   "2": 226.5392846,
                                                   "3": 218.7260749,
                                                   "4": 206.7594049}
                          }},
              "JJ6": {"SubmissionTime":    "Tue 21 Jul 2020, 18.04.01",
                      "Deadline":          "Fri 24 Jul 2020, 10.10.32",
                      "Priority":          1,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 217.7386529,
                                                   "2": 226.5392846,
                                                   "3": 218.7260749,
                                                   "4": 206.7594049}
                          }}
        },
     "nodes": {"n1": {"GPUtype":      "M60", 
                      "free_nGPUs":   4, 
                      "total_nGPUs":  4, 
                      "cost":         0.0},
               "n2": {"GPUtype":      "M60", 
                      "free_nGPUs":   4, 
                      "total_nGPUs":  4, 
                      "cost":         0.0}},
     "GPUcosts": {"K80": {"1": 0.56,
                          "2": 1.13,
                          "3": 1.69,
                          "4": 2.25,
                          "5": 2.8075,
                          "6": 3.3650,
                          "7": 3.9225,
                          "8": 4.48},
                  "M60": {"1": 0.62,
                          "2": 1.24,
                          "3": 1.86,
                          "4": 2.48,
                          "5": 3.10,
                          "6": 3.72,
                          "7": 4.34,
                          "8": 4.96}
        },
     "currentTime": "Tue 21 Jul 2020, 18.04.01",
     "verbose": 2
     }

    # expected result
    expected_result = {"estimated_cost": 101.73219999999999, 
                       "estimated_rescheduling_time": "Wed 22 Jul 2020, 14.34.39", 
                       "jobs": {"JJ1": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n2"}, 
                                "JJ2": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n2"}, 
                                "JJ3": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n2"}, 
                                "JJ5": {"expected_tardiness": 0.0, 
                                        "nGPUs": 4, 
                                        "node": "n1"}, 
                                "JJ6": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n2"}
                                }
                      }


  # third request - follows from the second one
  elif (req_id == 3):
    # data
    sample_data = {"jobs": {"JJ1": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Fri 24 Jul 2020, 07.16.50",
                      "Priority":          1,
                      "Epochs":            "661",
                      "ProfilingData":    {"M60": {"1": 217.7386529,
                                                   "2": 226.5392846,
                                                   "3": 218.7260749,
                                                   "4": 206.7594049}
                          }},
              "JJ4": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 19.34.15",
                      "Priority":          2,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 75.48717436,
                                                   "2": 81.05718885,
                                                   "3": 81.15076952,
                                                   "4": 79.87524173}
                          }},
              "JJ5": {"SubmissionTime":    "Tue 21 Jul 2020, 18.04.01",
                      "Deadline":          "Fri 24 Jul 2020, 04.37.12",
                      "Priority":          1,
                      "Epochs":            "643",
                      "ProfilingData":    {"M60": {"1": 217.7386529,
                                                   "2": 226.5392846,
                                                   "3": 218.7260749,
                                                   "4": 206.7594049}
                          }},
              "JJ6": {"SubmissionTime":    "Tue 21 Jul 2020, 18.04.01",
                      "Deadline":          "Fri 24 Jul 2020, 10.10.32",
                      "Priority":          1,
                      "Epochs":            "661",
                      "ProfilingData":    {"M60": {"1": 217.7386529,
                                                   "2": 226.5392846,
                                                   "3": 218.7260749,
                                                   "4": 206.7594049}
                          }}
        },
     "nodes": {"n1": {"GPUtype":      "M60", 
                      "free_nGPUs":   0, 
                      "total_nGPUs":  4, 
                      "cost":         0.0},
               "n2": {"GPUtype":      "M60", 
                      "free_nGPUs":   2, 
                      "total_nGPUs":  4, 
                      "cost":         0.0}},
     "GPUcosts": {"K80": {"1": 0.56,
                          "2": 1.13,
                          "3": 1.69,
                          "4": 2.25,
                          "5": 2.8075,
                          "6": 3.3650,
                          "7": 3.9225,
                          "8": 4.48},
                  "M60": {"1": 0.62,
                          "2": 1.24,
                          "3": 1.86,
                          "4": 2.48,
                          "5": 3.10,
                          "6": 3.72,
                          "7": 4.34,
                          "8": 4.96}
        },
     "currentScheduling":  {"JJ1": {"nGPUs": 1,
                                    "GPUtype": "M60", 
                                    "node": "n2"}, 
                            "JJ2": {"nGPUs": 1,
                                    "GPUtype": "M60",
                                    "node": "n2"}, 
                            "JJ3": {"nGPUs": 1,
                                    "GPUtype": "M60",
                                    "node": "n2"}, 
                            "JJ5": {"nGPUs": 4,
                                    "GPUtype": "M60",
                                    "node": "n1"}, 
                            "JJ6": {"nGPUs": 1,
                                    "GPUtype": "M60",
                                    "node": "n2"}
                           },
     "currentTime": "Wed 22 Jul 2020, 14.34.39",
     "verbose": 2
     }

    # expected result
    expected_result = {"estimated_cost": 91.00410000000001, 
                       "estimated_rescheduling_time": "Thu 23 Jul 2020, 11.32.46", 
                       "jobs": {"JJ1": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n2"}, 
                                "JJ4": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n2"}, 
                                "JJ5": {"expected_tardiness": 0.0, 
                                        "nGPUs": 4, 
                                        "node": "n1"}, 
                                "JJ6": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n2"}
                                }
                      }

  # fourth request - deadline violation
  elif (req_id == 4):
    # data
    sample_data = {"jobs": {"JJ4": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 19.34.15",
                      "Priority":          1,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 75.48717436,
                                                   "2": 81.05718885,
                                                   "3": 81.15076952,
                                                   "4": 79.87524173}
                          }}
        },
     "nodes": {"n1": {"GPUtype":      "M60", 
                      "free_nGPUs":   1, 
                      "total_nGPUs":  4, 
                      "cost":         0.0}},
     "GPUcosts": {"K80": {"1": 0.56,
                          "2": 1.13,
                          "3": 1.69,
                          "4": 2.25,
                          "5": 2.8075,
                          "6": 3.3650,
                          "7": 3.9225,
                          "8": 4.48},
                  "M60": {"1": 0.62,
                          "2": 1.24,
                          "3": 1.86,
                          "4": 2.48,
                          "5": 3.10,
                          "6": 3.72,
                          "7": 4.34,
                          "8": 4.96}
        },
     "currentTime": "Thu 23 Jul 2020, 08.54.39",
     "verbose": 2
     }

    # expected result
    expected_result = {"estimated_cost": 22.416, 
                       "estimated_rescheduling_time": "Fri 24 Jul 2020, 05.52.46", 
                       "jobs": {"JJ4": {"expected_tardiness": 10.308666666666666, 
                                        "nGPUs": 1, 
                                        "node": "n1"}}
                      }

  # fifth request - heterogeneous cluster
  elif (req_id == 5):
    # data
    sample_data = {"jobs": {"JJ1": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Fri 24 Jul 2020, 07.16.50",
                      "Priority":          1,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 217.7386529,
                                                   "2": 226.5392846,
                                                   "3": 218.7260749,
                                                   "4": 206.7594049},
                                           "K80": {"1": 239.6462542,
                                                   "2": 228.5185733,
                                                   "4": 189.7872526,
                                                   "8": 104.086632}
                          }},
              "JJ2": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 13.37.01",
                      "Priority":          4,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 73.83799561,
                                                   "2": 77.89974382999999,
                                                   "3": 76.45150798,
                                                   "4": 73.62577612000001},
                                           "K80": {"1": 81.99057825,
                                                   "4": 69.56075288,
                                                   "8": 47.07705637}
                          }},
              "JJ3": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 10.59.50",
                      "Priority":          3,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 73.83799561,
                                                   "2": 77.89974382999999,
                                                   "3": 76.45150798,
                                                   "4": 73.62577612000001},
                                           "K80": {"1": 81.99057825,
                                                   "2": 79.53604485,
                                                   "4": 69.56075288}
                          }},
              "JJ4": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 19.34.15",
                      "Priority":          2,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 75.48717436,
                                                   "2": 81.05718885,
                                                   "3": 81.15076952,
                                                   "4": 79.87524173},
                                           "K80": {"1": 84.77789652000001,
                                                   "2": 83.99986464,
                                                   "3": 81.21997125,
                                                   "4": 77.93961249,
                                                   "5": 74.45906757,
                                                   "6": 70.87842958,
                                                   "7": 67.24059555,
                                                   "8": 63.56701399}
                          }},
              "JJ5": {"SubmissionTime":    "Tue 21 Jul 2020, 18.04.01",
                      "Deadline":          "Fri 24 Jul 2020, 04.37.12",
                      "Priority":          1,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 217.7386529,
                                                   "2": 226.5392846,
                                                   "3": 218.7260749,
                                                   "4": 206.7594049},
                                           "K80": {"1": 239.6462542,
                                                   "2": 228.5185733,
                                                   "4": 189.7872526,
                                                   "8": 104.086632}
                          }},
              "JJ6": {"SubmissionTime":    "Tue 21 Jul 2020, 18.04.01",
                      "Deadline":          "Fri 24 Jul 2020, 10.10.32",
                      "Priority":          5,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 217.7386529,
                                                   "2": 226.5392846,
                                                   "3": 218.7260749,
                                                   "4": 206.7594049},
                                           "K80": {"1": 239.6462542,
                                                   "8": 104.086632}
                          }}
        },
     "nodes": {"n1": {"GPUtype":      "M60", 
                      "free_nGPUs":   4, 
                      "total_nGPUs":  4, 
                      "cost":         0.0},
               "n2": {"GPUtype":      "K80", 
                      "free_nGPUs":   8, 
                      "total_nGPUs":  8, 
                      "cost":         0.0}},
     "GPUcosts": {"K80": {"1": 0.56,
                          "2": 1.13,
                          "3": 1.69,
                          "4": 2.25,
                          "5": 2.8075,
                          "6": 3.3650,
                          "7": 3.9225,
                          "8": 4.48},
                  "M60": {"1": 0.62,
                          "2": 1.24,
                          "3": 1.86,
                          "4": 2.48,
                          "5": 3.10,
                          "6": 3.72,
                          "7": 4.34,
                          "8": 4.96}
        },
     "currentTime": "Tue 21 Jul 2020, 18.04.01",
     "verbose": 2
     }

    # expected result
    expected_result = {"estimated_cost": 97.01469999999999, 
                       "estimated_rescheduling_time": "Wed 22 Jul 2020, 14.34.39", 
                       "jobs": {"JJ1": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n1"}, 
                                "JJ3": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n1"}, 
                                "JJ4": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n1"}, 
                                "JJ5": {"expected_tardiness": 0.0, 
                                        "nGPUs": 4, 
                                        "node": "n2"}, 
                                "JJ6": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n1"}}
                      }

  # sixth request - different method
  elif (req_id == 6):
    # data
    sample_data = {"jobs": {"JJ4": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 19.34.15",
                      "Priority":          5,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 75.48717436,
                                                   "2": 81.05718885,
                                                   "3": 81.15076952,
                                                   "4": 79.87524173}
                          }}
        },
     "nodes": {"n1": {"GPUtype":      "M60", 
                      "free_nGPUs":   1, 
                      "total_nGPUs":  4, 
                      "cost":         0.0}},
     "GPUcosts": {"K80": {"1": 0.56,
                          "2": 1.13,
                          "3": 1.69,
                          "4": 2.25,
                          "5": 2.8075,
                          "6": 3.3650,
                          "7": 3.9225,
                          "8": 4.48},
                  "M60": {"1": 0.62,
                          "2": 1.24,
                          "3": 1.86,
                          "4": 2.48,
                          "5": 3.10,
                          "6": 3.72,
                          "7": 4.34,
                          "8": 4.96}
        },
     "currentTime": "Wed 22 Jul 2020, 14.34.39",
     "method": "EDF",
     "verbose": 3
     }

    # expected result
    expected_result = {"estimated_cost": 13.0006, 
                       "estimated_rescheduling_time": "Thu 23 Jul 2020, 11.32.46", 
                       "jobs": {"JJ4": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n1"}}
                      }

  # seventh request - missing field in data
  elif (req_id == 7):
    # data
    sample_data = {"jobs": {"JJ4": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 19.34.15",
                      "Priority":          1,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 75.48717436,
                                                   "2": 81.05718885,
                                                   "3": 81.15076952,
                                                   "4": 79.87524173}
                          }}
        },
     "currentTime": "Wed 22 Jul 2020, 14.34.39",
     "verbose": 2,
     "method": "RG"
     }

    # expected result
    expected_result = {"ERROR CODE": 424}

  # eight request - tardiness weight equal to zero
  elif (req_id == 8):
    # data
    sample_data = {"jobs": {"JJ1": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Fri 24 Jul 2020, 07.16.50",
                      "Priority":          1,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 217.7386529,
                                                   "2": 226.5392846,
                                                   "3": 218.7260749,
                                                   "4": 206.7594049}
                          }},
              "JJ2": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 13.37.01",
                      "Priority":          3,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 73.83799561,
                                                   "2": 77.89974382999999,
                                                   "3": 76.45150798,
                                                   "4": 73.62577612000001}
                          }},
              "JJ3": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 10.59.50",
                      "Priority":          0,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 73.83799561,
                                                   "2": 77.89974382999999,
                                                   "3": 76.45150798,
                                                   "4": 73.62577612000001}
                          }},
              "JJ4": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 19.34.15",
                      "Priority":          2,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 75.48717436,
                                                   "2": 81.05718885,
                                                   "3": 81.15076952,
                                                   "4": 79.87524173}
                          }}
        },
     "nodes": {"n1": {"GPUtype":      "M60", 
                      "free_nGPUs":   4, 
                      "total_nGPUs":  4, 
                      "cost":         0.0},
               "n2": {"GPUtype":      "M60", 
                      "free_nGPUs":   4, 
                      "total_nGPUs":  4, 
                      "cost":         0.0}},
     "GPUcosts": {"K80": {"1": 0.56,
                          "2": 1.13,
                          "3": 1.69,
                          "4": 2.25,
                          "5": 2.8075,
                          "6": 3.3650,
                          "7": 3.9225,
                          "8": 4.48},
                  "M60": {"1": 0.62,
                          "2": 1.24,
                          "3": 1.86,
                          "4": 2.48,
                          "5": 3.10,
                          "6": 3.72,
                          "7": 4.34,
                          "8": 4.96}
        },
     "currentTime": "Tue 21 Jul 2020, 17.56.59",
     "verbose": 2
     }

    # expected result
    expected_result = {"ERROR CODE": 444}

  # ninth request - partial (but sufficient) profiling data
  elif (req_id == 9):
    # data
    sample_data = {"jobs": {"JJ1": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Fri 24 Jul 2020, 07.16.50",
                      "Priority":          1,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 217.7386529,
                                                   "2": 226.5392846,
                                                   "3": 218.7260749,
                                                   "4": 206.7594049}
                          }},
              "JJ2": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 13.37.01",
                      "Priority":          4,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 73.83799561,
                                                   "2": 77.89974382999999,
                                                   "3": 76.45150798,
                                                   "4": 73.62577612000001}
                          }},
              "JJ3": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 10.59.50",
                      "Priority":          4,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 73.83799561,
                                                   "2": 77.89974382999999,
                                                   "3": 76.45150798,
                                                   "4": 73.62577612000001}
                          }},
              "JJ4": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 19.34.15",
                      "Priority":          2,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 75.48717436,
                                                   "2": 81.05718885,
                                                   "3": 81.15076952,
                                                   "4": 79.87524173}
                          }}
        },
     "nodes": {"n1": {"GPUtype":      "M60", 
                      "free_nGPUs":   4, 
                      "total_nGPUs":  4, 
                      "cost":         0.0},
               "n2": {"GPUtype":      "K80", 
                      "free_nGPUs":   8, 
                      "total_nGPUs":  8, 
                      "cost":         0.0}},
     "GPUcosts": {"K80": {"1": 0.56,
                          "2": 1.13,
                          "3": 1.69,
                          "4": 2.25,
                          "5": 2.8075,
                          "6": 3.3650,
                          "7": 3.9225,
                          "8": 4.48},
                  "M60": {"1": 0.62,
                          "2": 1.24,
                          "3": 1.86,
                          "4": 2.48,
                          "5": 3.10,
                          "6": 3.72,
                          "7": 4.34,
                          "8": 4.96}
        },
     "currentTime": "Tue 21 Jul 2020, 17.56.59",
     "verbose": 2
     }

    # expected result
    expected_result = {"estimated_cost": 50.866, 
                       "estimated_rescheduling_time": "Wed 22 Jul 2020, 14.27.37", 
                       "jobs": {"JJ1": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n1"}, 
                                "JJ2": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n1"}, 
                                "JJ3": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n1"}, 
                                "JJ4": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n1"}}
                      }

  # tenth request - missing profiling data
  elif (req_id == 10):
    # data
    sample_data = {"jobs": {"JJ1": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Fri 24 Jul 2020, 07.16.50",
                      "Priority":          1,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 217.7386529,
                                                   "2": 226.5392846,
                                                   "3": 218.7260749,
                                                   "4": 206.7594049}
                          }},
              "JJ2": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 13.37.01",
                      "Priority":          4,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 73.83799561,
                                                   "2": 77.89974382999999,
                                                   "3": 76.45150798,
                                                   "4": 73.62577612000001}
                          }},
              "JJ3": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 10.59.50",
                      "Priority":          4,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 73.83799561,
                                                   "2": 77.89974382999999,
                                                   "3": 76.45150798,
                                                   "4": 73.62577612000001}
                          }},
              "JJ4": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 19.34.15",
                      "Priority":          2,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 75.48717436,
                                                   "2": 81.05718885,
                                                   "3": 81.15076952,
                                                   "4": 79.87524173}
                          }}
        },
     "nodes": {"n1": {"GPUtype":      "K80", 
                      "free_nGPUs":   8, 
                      "total_nGPUs":  8, 
                      "cost":         0.0},
               "n2": {"GPUtype":      "K80", 
                      "free_nGPUs":   8, 
                      "total_nGPUs":  8, 
                      "cost":         0.0}},
     "GPUcosts": {"K80": {"1": 0.56,
                          "2": 1.13,
                          "3": 1.69,
                          "4": 2.25,
                          "5": 2.8075,
                          "6": 3.3650,
                          "7": 3.9225,
                          "8": 4.48},
                  "M60": {"1": 0.62,
                          "2": 1.24,
                          "3": 1.86,
                          "4": 2.48,
                          "5": 3.10,
                          "6": 3.72,
                          "7": 4.34,
                          "8": 4.96}
        },
     "currentTime": "Tue 21 Jul 2020, 17.56.59",
     "verbose": 2
     }

    # expected result
    expected_result = {"ERROR CODE": 454}

  # eleventh request - simulator
  elif (req_id == 11):
    # data
    sample_data = {"jobs": {"JJ1": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Fri 24 Jul 2020, 07.16.50",
                      "Priority":          2,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 217.7386529,
                                                   "2": 226.5392846,
                                                   "3": 218.7260749,
                                                   "4": 206.7594049}
                          }},
              "JJ2": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 13.37.01",
                      "Priority":          4,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 73.83799561,
                                                   "2": 77.89974382999999,
                                                   "3": 76.45150798,
                                                   "4": 73.62577612000001}
                          }},
              "JJ3": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 10.59.50",
                      "Priority":          3,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 73.83799561,
                                                   "2": 77.89974382999999,
                                                   "3": 76.45150798,
                                                   "4": 73.62577612000001}
                          }},
              "JJ4": {"SubmissionTime":    "Tue 21 Jul 2020, 17.56.59",
                      "Deadline":          "Thu 23 Jul 2020, 19.34.15",
                      "Priority":          2,
                      "Epochs":            "1000",
                      "ProfilingData":    {"M60": {"1": 75.48717436,
                                                   "2": 81.05718885,
                                                   "3": 81.15076952,
                                                   "4": 79.87524173}
                          }}
        },
     "nodes": {"n1": {"GPUtype":      "M60", 
                      "free_nGPUs":   4, 
                      "total_nGPUs":  4, 
                      "cost":         0.0},
               "n2": {"GPUtype":      "M60", 
                      "free_nGPUs":   4, 
                      "total_nGPUs":  4, 
                      "cost":         0.0}},
     "GPUcosts": {"K80": {"1": 0.56,
                          "2": 1.13,
                          "3": 1.69,
                          "4": 2.25,
                          "5": 2.8075,
                          "6": 3.3650,
                          "7": 3.9225,
                          "8": 4.48},
                  "M60": {"1": 0.62,
                          "2": 1.24,
                          "3": 1.86,
                          "4": 2.48,
                          "5": 3.10,
                          "6": 3.72,
                          "7": 4.34,
                          "8": 4.96}
        },
     "currentTime": "Tue 21 Jul 2020, 17.56.59",
     "verbose": 2,
     "method": "G_sim"
     }

    # expected result
    expected_result = {"estimated_cost": 50.866, 
                       "estimated_rescheduling_time": "Wed 22 Jul 2020, 14.27.37", 
                       "jobs": {"JJ1": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n1"}, 
                                "JJ2": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n1"}, 
                                "JJ3": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n1"}, 
                                "JJ4": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "n1"}
                                }
                      }

  # twelfth request - real example (1)
  elif (req_id == 12):
    # data
    sample_data = {"currentTime":"Wed 24 Mar 2021, 13.43.10",
                   "jobs":{"1":  {"SubmissionTime":"Wed 24 Mar 2021, 13.35.09",
                                  "Deadline":"Wed 28 Oct 2020, 17.09.42",
                                  "Priority":1,
                                  "Epochs":12,
                                  "ProfilingData":  {"TESLA": {"1":100.073}}}
                          },
                   "nodes": {"armida-04": {"GPUtype": "TESLA",
                                           "free_nGPUs":0,
                                           "total_nGPUs":1,
                                           "cost":4.48
                                          },
                             "armida-05": {"GPUtype": "TESLA",
                                           "free_nGPUs":1,
                                           "total_nGPUs":1,
                                           "cost":3.14}
                                          },
                   "currentScheduling": {"1": {"node":  "armida-04",
                                               "GPUtype": "TESLA",
                                               "nGPUs": 1}
                                        },
                   "GPUcosts":  {"TESLA": {"1": 0.12101944,
                                           "2": 0.159837},
                                 "TURING":  {"1": 0.09133542}
                                },
                   "verbose":2
                  }

    # expected result
    expected_result = {"estimated_cost": 3219.52, 
                    "estimated_rescheduling_time": "Wed 24 Mar 2021, 14.03.10", 
                    "jobs": {"1": {"expected_tardiness": 3524.8888888888887, 
                                   "nGPUs": 1, 
                                   "node": "armida-04"}}
                    }

  # tirteenth request - real example (2)
  elif (req_id == 13):
    # data
    sample_data = {"currentTime":"Mon 29 Mar 2021, 12.06.07",
                   "jobs":  {"175": {"SubmissionTime":"Mon 29 Mar 2021, 11.42.17",
                                     "Deadline":"Mon 29 Mar 2021, 12.09.42",
                                     "Priority":1,
                                     "Epochs":5,
                                     "ProfilingData": {"TESLA":{"1":86.1135}}}
                                     },
                   "nodes": {"armida-07": {"GPUtype":"TURING",
                                           "free_nGPUs":1,
                                           "total_nGPUs":1,
                                           "cost":0.00},
                             "armida-05": {"GPUtype":"TESLA",
                                           "free_nGPUs":0,
                                           "total_nGPUs":1,
                                           "cost":0.00},
                             "armida-06": {"GPUtype":"TESLA",
                                           "free_nGPUs":2,
                                           "total_nGPUs":2,
                                           "cost":0.00}
                                           },
                   "currentScheduling": {"175": {"node":"armida-05",
                                                 "GPUtype":
                                                 "TESLA","nGPUs":1}
                                                 },
                   "GPUcosts":  {"TESLA": {"1":0.12101944,
                                           "2":0.159837},
                                 "TURING":{"1":0.09133542}
                                 },
                   "verbose": 2,
                   "method":"EDF"
              }

    # expected result
    expected_result = {"estimated_cost": 0.06916580760186813,
                       "estimated_rescheduling_time": "Mon 29 Mar 2021, 12.13.17",
                       "jobs": {"175": {"expected_tardiness": 215.56750011444092,
                                        "nGPUs": 1,
                                        "node": "armida-05"}
                                }
                      }

  # fourteenth request - real example (3)
  elif (req_id == 14):
    # data
    sample_data = {"jobs": {"J6": {"SubmissionTime":    "Thu 1 Jan 1970, 1.0.0",
                                   "Deadline":          "Thu 1 Jan 1970, 3.0.0",
                                   "Priority":          2,
                                   "Epochs":            "80",
                                   "ProfilingData":    {"TESLA": {"1": 51.6221,
                                                                  "2": 41.2372},
                                                        "TURING": {"1": 88.9881}
                                  }}
                    },
                   "nodes": {"armida-05": {"GPUtype":      "TESLA", 
                                           "free_nGPUs":   1, 
                                           "total_nGPUs":  1, 
                                           "cost":         0.0},
                             "armida-06": {"GPUtype":      "TESLA", 
                                           "free_nGPUs":   2, 
                                           "total_nGPUs":  2, 
                                           "cost":         0.0},
                             "armida-07": {"GPUtype":      "TURING", 
                                           "free_nGPUs":   1, 
                                           "total_nGPUs":  1, 
                                           "cost":         0.0}},
                   "GPUcosts": {"TURING": {"1": 0.09133542516},
                                "TESLA":  {"1": 0.0909920589,
                                           "2": 0.120178191}
                      },
                   "currentTime": "Thu 1 Jan 1970, 1.0.0",
                   "verbose": 3,
                   "method": "EDF"
     }

    # expected result
    expected_result = {"estimated_cost": 0.10438199999999999, 
                    "estimated_rescheduling_time": "Thu 01 Jan 1970, 02.08.49", 
                       "jobs": {"J6": {"expected_tardiness": 0.0, 
                                       "nGPUs": 1, 
                                       "node": "armida-05"}
                                       }
                      }

  # fifteenth request - real example (4)
  elif (req_id == 15):
    # data
    sample_data = {"jobs": {"J6": {"SubmissionTime":    "Thu 1 Jan 1970, 1.0.0",
                                   "Deadline":          "Thu 1 Jan 1970, 3.0.0",
                                   "Priority":          2,
                                   "Epochs":            "57",
                                   "ProfilingData":    {"TESLA": {"1": 51.6221,
                                                                  "2": 41.2372},
                                                        "TURING": {"1": 88.9881}
                                  }},
                            "J9": {"SubmissionTime":    "Thu 1 Jan 1970, 1.20.0",
                                   "Deadline":          "Thu 1 Jan 1970, 2.43.20",
                                   "Priority":          1,
                                   "Epochs":            "160",
                                   "ProfilingData":    {"TESLA": {"1": 20.5382,
                                                                  "2": 17.8042},
                                                        "TURING": {"1": 41.2582}
                                  }},
                    },
                   "nodes": {"armida-05": {"GPUtype":      "TESLA", 
                                           "free_nGPUs":   0, 
                                           "total_nGPUs":  1, 
                                           "cost":         0.0},
                             "armida-06": {"GPUtype":      "TESLA", 
                                           "free_nGPUs":   2, 
                                           "total_nGPUs":  2, 
                                           "cost":         0.0},
                             "armida-07": {"GPUtype":      "TURING", 
                                           "free_nGPUs":   1, 
                                           "total_nGPUs":  1, 
                                           "cost":         0.0}},
                   "GPUcosts": {"TURING": {"1": 0.09133542516},
                                "TESLA":  {"1": 0.0909920589,
                                           "2": 0.120178191}
                      },
                   "currentTime": "Thu 1 Jan 1970, 1.20.0",
                   "currentScheduling": {"J6": {"nGPUs": 1, 
                                                "GPUtype": "TESLA",
                                                "node": "armida-05"}},
                   "verbose": 3,
                   "method": "EDF"
     }

    # expected result
    expected_result = {"estimated_cost": 0.15743075175924343, 
                    "estimated_rescheduling_time": "Thu 01 Jan 1970, 02.09.02", 
                       "jobs": {"J6": {"expected_tardiness": 0, 
                                       "nGPUs": 1, 
                                       "node": "armida-05"}, 
                                "J9": {"expected_tardiness": 0.0, 
                                       "nGPUs": 1, 
                                       "node": "armida-06"}
                                       }
                        }

  # sixteenth request - real example (5)
  elif (req_id == 16):
    # data
    sample_data = {"jobs": {"J6": {"SubmissionTime":    "Thu 1 Jan 1970, 1.0.0",
                                   "Deadline":          "Thu 1 Jan 1970, 3.0.0",
                                   "Priority":          2,
                                   "Epochs":            "41",
                                   "ProfilingData":    {"TESLA": {"1": 51.6221,
                                                                  "2": 41.2372},
                                                        "TURING": {"1": 88.9881}
                                  }},
                            "J9": {"SubmissionTime":    "Thu 1 Jan 1970, 1.20.0",
                                   "Deadline":          "Thu 1 Jan 1970, 2.43.20",
                                   "Priority":          1,
                                   "Epochs":            "102",
                                   "ProfilingData":    {"TESLA": {"1": 20.5382,
                                                                  "2": 17.8042},
                                                        "TURING": {"1": 41.2582}
                                  }},
                            "J10": {"SubmissionTime":   "Thu 1 Jan 1970, 1.40.0",
                                   "Deadline":          "Wed 17 Mar 2021, 4.06.40",
                                   "Priority":          2,
                                   "Epochs":            "80",
                                   "ProfilingData":    {"TESLA": {"1": 20.5382,
                                                                  "2": 17.8042},
                                                        "TURING": {"1": 41.2582}
                                  }},
                    },
                   "nodes": {"armida-05": {"GPUtype":      "TESLA", 
                                           "free_nGPUs":   0, 
                                           "total_nGPUs":  1, 
                                           "cost":         0.0},
                             "armida-06": {"GPUtype":      "TESLA", 
                                           "free_nGPUs":   1, 
                                           "total_nGPUs":  2, 
                                           "cost":         0.0},
                             "armida-07": {"GPUtype":      "TURING", 
                                           "free_nGPUs":   1, 
                                           "total_nGPUs":  1, 
                                           "cost":         0.0}},
                   "GPUcosts": {"TURING": {"1": 0.09133542516},
                                "TESLA":  {"1": 0.0909920589,
                                           "2": 0.120178191}
                      },
                   "currentTime": "Thu 1 Jan 1970, 1.40.0",
                   "currentScheduling": {"J6": {"nGPUs": 1,
                                                "GPUtype": "TESLA",
                                                "node": "armida-05"},
                                         "J9": {"nGPUs": 1, 
                                                "GPUtype": "TESLA",
                                                "node": "armida-06"}},
                   "verbose": 3,
                   "method": "EDF"
     }

    # expected result
    expected_result = {"estimated_cost": 0.1479748067587798, 
                    "estimated_rescheduling_time": "Thu 01 Jan 1970, 02.07.23", 
                       "jobs": {"J10": {"expected_tardiness": 0.0, 
                                        "nGPUs": 1, 
                                        "node": "armida-06"}, 
                                "J6": {"expected_tardiness": 0, 
                                       "nGPUs": 1, 
                                       "node": "armida-05"}, 
                                "J9": {"expected_tardiness": 0, 
                                       "nGPUs": 1, 
                                       "node": "armida-06"}
                                       }
                        }


  else:
    print("ERROR: request id not found")
    sys.exit(-1)

  sample_result = requests.post(url = url, json = sample_data)
  print("EXPECTED RESULT")
  print(expected_result)
  print("\nRESULT")
  print(sample_result)
  print(sample_result.json())


def list_possible_tests ():
  print("1:   generic test (few jobs)")
  print("2:   generic test (more jobs; no enough resources in the system)")
  print("3:   generic test (current time is the end time of test 2)")
  print("4:   deadline violation")
  print("5:   heterogeneous cluster")
  print("6:   different method")
  print("7:   missing field in data")
  print("8:   tardiness weight equal to zero")
  print("9:   partial (but sufficient) profiling data")
  print("10:  missing profiling data")
  print("11:  simulator")
  print("12:  real example")
  print("13:  real example")
  print("14:  real example")
  print("15:  real example")
  print("16:  real example")


if __name__ == "__main__":
  if sys.argv[1] == "-h" or sys.argv[1] == "--help":
    list_possible_tests()
  else:
    port = int(sys.argv[1])
    req_id = int(sys.argv[2])
    main(port, req_id)
