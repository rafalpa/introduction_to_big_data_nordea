#!/bin/bash
hadoop jar hadoop-streaming.jar \
  -input /user/root/weather_data/*.gz \
  -output output_py \
  -mapper max_temperature_map.py \
  -reducer max_temperature_reduce.py \
  -file max_temperature_map.py \
  -file max_temperature_reduce.py


#hadoop jar hadoop-streaming.jar -input /user/root/weather_data/*.gz -output output_py -mapper max_temperature_map.py -reducer max_temperature_reduce.py -file max_temperature_map.py -file max_temperature_reduce.py
