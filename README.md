# Big-data-group-project

**Purpose of this project** is to detect marine transportation ports in the dataset.
**The tasks** to achieve this goal are:

## 1. Filtering out the noise and preparing data for port detection
## 2. Creating an algorithm(s) for port detection
### a) Using a K-means clustering algorithm
Initially, the optimal cluster size was determined using the elbow method, which suggested either 4 or 7 clusters. Subsequently, clustering was also performed with 15 and 20 clusters.
## 3. Evaluation of the relative size of the port
## 4. Port visualization

Ports were visualized using the ```folium``` library, creating interactive graphs with ports represented as bubbles. These bubbles indicate the port size, having the number of vessels present in each port. The maps were saved as ```html``` files, which can be found on GitHub.

