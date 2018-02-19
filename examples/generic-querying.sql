SELECT dataid FROM commercial.metadata WHERE egauge_min_time <= %s AND egauge_max_time >= %s LIMIT 10;
