f_write = open('result.txt', 'w')

for i in range(1, 10358):
    taxi_id = i
    filename = str(i) + '.txt'
    f_read = open(filename, 'r')
    lines = f_read.readlines()
    line_1 = lines[0]
    line_1 = line_1.rstrip().split(",")
    # print(line_1)
    ts_start = line_1[1]
    p_start = 'POINT(' + line_1[2] + ' ' + line_1[3] + ')'
    for j in range(1, len(lines)):
        line = lines[j].rstrip().split(",")
        ts_end = line[1]
        p_end = 'POINT(' + line[2] + ' ' + line[3] + ')'
        period = '[' + ts_start + ', ' + ts_end + ')'
        query = "INSERT INTO taxi_trip(taxi_id, p_start, p_end, period) VALUES (" + str(taxi_id) + ", '" + p_start + "', '" + p_end + "', '" + period + "');\n"
        f_write.write(query)
        ts_start = ts_end
        p_start = p_end
    break
