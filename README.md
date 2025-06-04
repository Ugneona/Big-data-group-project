# Big-data-group-project

**Purpose of this project** is to detect marine transportation ports in the dataset. The tasks were implemented using pyspark.

**The tasks** to achieve this goal are:

## 1. Filtering out the noise and preparing data for port detection
**General Filtering Logic:**

- Only take useful columns, drop NA values, cast to appropriate data types.
- Remove ships with non-standard coordinate types and helicopters by filtering the "Ship type" column.


**Filtering For Port Detection Logic:**

- Filter out moving vessels that send GPS messages for speed of 0.5 by "SOG" or higher for more than 5 times. The "SOG" < 0.5 decision was made to include nearby "waiting to port" ships. For message count > 5 decision was made to exclude ships "traveling in chanel" and slowing down for a small period of time.
- Filter out vessels that have traveled more than 10 kilometers in the given day. Traveled < 10km decision was made to include "fishing near port" ships.
## 2. Creating an algorithm(s) for port detection
### a) Spatial binning algorithm
Spatial binning clusters vessels geographically by grouping them into grid cells based on their latitude and longitude coordinates. By rounding these coordinates to one decimal place, it assigns each vessel to a spatial bin representing an approximate 0.64 x 1.11km area (roughly 0.7km squared) (may differ depending on latitude and longitude). Vessels located within the same bin are treated as part of the same cluster. 

Additionally, a bigger grid size was tested - approximately 6.4 × 11.1km rectangles (roughly 71km squared), but showed worse results.
### b) K-means clustering algorithm
Initially, the optimal cluster size was determined using the elbow method, which suggested either 4 or 7 clusters. Subsequently, clustering was also performed with 15 and 20 clusters. Only clusters with more than 20 vessels were left, to filter out fishing vessel spots.
### c) DBSCAN clustering algorithm

DBSCAN was used to cluster Spacial Binning clusters to larger ports. This allowed to give each port just one port/terminal as opposed to multiple that was seen in just Spacial Binning method.

The maximum distance between two samples for one to be considered as in the neighborhood was selected as 0.1, equal to approximately 11 kilometers, while minimum samples in a cluster was set to 1, as some ports were correctly identified with Spacial Binning.  
## 3. Evaluation of the relative size of the port
### a) Spatial binning algorithm
TOP 5 ports contained between 34 and 56 vessels.
### b) K-means clustering algorithm
For the K-means clustering algorithm, relative port sizes ranged from 40 to 450 vessels using 19 ports.
### c)  DBSCAN clustering algorithm
TOP 5 ports were København, Hirtshals, Frederikshavn, Skagen, and Heiligenhafen containing between 62 and 131 vessels.
## 4. Port visualization

Ports were visualized using the ```folium``` library, creating interactive graphs with ports represented as bubbles. These bubbles indicate the port size, with the number of vessels in each port. The maps were saved as ```html``` files, which can be found on GitHub.

## Conclusion
<mark>The best method to detect marine transportation ports was using DBSCAN clustering, see ```dbscan_ports.html```</mark>
