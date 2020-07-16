# Homework Big Data Computing

This repository hosts my homework for the Big Data Computing course at University of Padova (UNIPD).
These 3 exercises were evaluated as:

- hw1: 2/2 points
- hw2: 3/3 points
- hw3: 3/3 points

Moreover, we received an additional point for submitting every exercise before the deadline.

## Technology Stack

The homework is implemented in Java 8.
I've refactored the three exercises into a single Gradle multi-project.

This project has been tested on:

- Apache Spark 2.4.5
- Apache Hadoop 2.7
- JVM 1.8
- Gradle 6.1

If you want to run this project locally, you need to add the following VM options:

- `-Dspark.driver.host=localhost`
- `-Dspark.master="local[*]"`

## HW1 - Class count problem

- [Original assignment](./assignments/hw1.pdf)
- [Solution subproject](./hw1/src/main/java/hw1)
- [Datasets](./datasets/hw1)

### Example

Example configuration:
- dataset: [input_10000.txt](./datasets/hw1/input_10000.txt)
- partitions: 4

![HW1 example configuration](images/hw1.png?raw=true "HW1 example configuration")

Example output:

```txt
VERSION WITH DETERMINISTIC PARTITIONS
Output pairs = (Action,846) (Animation,845) (Comedy,863) (Crime,818) (Drama,870) (Fantasy,871) (Horror,3055) (Romance,864) (SciFi,877) (Thriller,91) 

VERSION WITH SPARK PARTITIONS
Most frequent class = (Horror,3055)
Max partition size = 2500
```

## HW2 - Maximum pairwise distance problem

- [Original assignment](./assignments/hw2.pdf)
- [Solution subproject](./hw2/src/main/java/hw2)
- [Datasets](./datasets/hw2)

### Example

Example configuration:
- dataset: [uber-small.csv](./datasets/hw2/uber-small.csv)
- K: 8

![HW2 example configuration](images/hw2.png?raw=true "HW2 example configuration")

Example output:

```txt
EXACT ALGORITHM
Max distance = 0.999805446074393
Running time = 164

2-APPROXIMATION ALGORITHM
k = 8
Max distance = 0.614674588705289
Running time = 3

k-CENTER-BASED ALGORITHM
k = 8
Max distance = 0.999805446074393
Running time = 8
```

### Notes

I also included a particular case of exact maximum pairwise distance calculator that works in `O(n logn)` for vectors of 2 dimensions.
It works much faster than the brute-force `O(n^2)` solution, and uses the following approach:

- first, it calculates the Convex Hull of the data points in `O(n logn)`
- then, it calculates the Hull diameter using the Rotating Calipers technique in `O(n)`
- the diameter of the Convex Hull is the maximum pairwise distance

## HW3 - Diversity maximization problem

- [Original assignment](./assignments/hw3.pdf)
- [Solution subproject](./hw3/src/main/java/hw3)
- [Datasets](./datasets/hw3)
- [Report](./hw3-report.pdf)

### Example

Example configuration:
- dataset: [zalando.csv](./datasets/hw3/zalando.csv)
- K: 10
- L: 8

![HW3 example configuration](images/hw3.png?raw=true "HW3 example configuration")

Example output:

```txt
Number of points = 60000
k = 10
L = 8
Initialization time = 5471

Runtime of Round 1 = 841
Runtime of Round 2 = 46

Average distance = 4238.642362476659000
```

### Notes

- This homework was run as in the CloudVeneto cluster using the dataset Glove2M300d, which hasn't been included
because it's 5.2GB.
- The professors expected the configuration `(k=100, l=4, Spark_executors=4)` to fail on the cluster with an `OutOfMemoryError`. However, our implementation of `kCenterMPD` is particularly space-efficient.

## Authors

- Alberto Schiabel ([jkomyno](https://github.com/jkomyno))
- Bryan Lucchetta ([1-coder](https://github.com/1-coder))
- Elisa Sartori ([elykka](https://github.com/elykka))
