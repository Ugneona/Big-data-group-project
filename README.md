# Big-data-group-project

**Purpose of this project** is to detect marine transportation ports in the dataset.
**The tasks** to achieve this goal are:

## 1. Filtering out the noise and preparing data for port detection
## 2. Creating an algorithm(s) for port detection
### a) Spatial binning algorithm
Spatial binning clusters vessels geographically by grouping them into grid cells based on their latitude and longitude coordinates. By rounding these coordinates to one decimal place, it assigns each vessel to a spatial bin representing an approximate 0.64 x 1.11km area (roughly 0.7km squared) (may differ depending on latitude and longitude). Vessels located within the same bin are treated as part of the same cluster. 

Additionally, a bigger grid size was tested - approximately 6.4 × 11.1km rectangles (roughly 71km squared), but showed worse results.
### b) K-means clustering algorithm
Initially, the optimal cluster size was determined using the elbow method, which suggested either 4 or 7 clusters. Subsequently, clustering was also performed with 15 and 20 clusters. Only clusters with more than 20 vessels were left, to filter out fishing vessel spots.
## 3. Evaluation of the relative size of the port
### b) K-means clustering algorithm
For the K-means clustering algorithm, relative port sizes ranged from 40 to 450 vessels using 19 ports.
## 4. Port visualization

Ports were visualized using the ```folium``` library, creating interactive graphs with ports represented as bubbles. These bubbles indicate the port size, with the number of vessels in each port. The maps were saved as ```html``` files, which can be found on GitHub.

