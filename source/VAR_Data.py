import numpy as np
import math
import datetime as dt

if __name__ == '__main__':
    k = 10
    np.set_printoptions(suppress=True)
    noise = np.random.normal(0, 1, k)
    noise2 = np.random.normal(0, 1, k)
    # print(noise)
    # print(noise2)
    source1 = np.zeros(k)
    source1[1] = noise[1] + 10
    source1[2] = noise[2] + 10
    source2 = np.zeros(k)
    source2[1] = noise2[1]
    source2[2] = noise2[2]
    f = open("./synthautoreg.csv", "a+")
    # lmt = 9999999999999999
    print("Starting the Data Generation")
    for x in range(3, k):
        source1[x] = 0.95 * math.sqrt(2) * source1[x - 1] - 0.90 * source1[x - 2] + noise[x]
        source2[x] = 0.5 * source2[x - 2] + noise2[x]
        msg = str(source1[x]) + ',' + str(source2[x]) + "\n"
        f.write(msg)
    f.close()
