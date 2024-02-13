# Mineral Site Linking (FuseMine?) Experiment Log

## ver. 0 (Last Updated: 01/15/2024)
### Description:
Mineral site record linking method based only on geolocations
### Details:
- Interlinking uses 0.005 bound HDBSCAN
- Linked regions (either single point, line, or polygon) has a 10 meter buffer applied
- Selects the maximum overlapping record (across database) that has at least 1 $m^2$ overlapping region
### Note:
- This method would be applied as a base to all other methods (a.k.a. ver 1, ver 2, ver 3)
    - HDBSCAN bound will increase to 0.05 to increase completeness
- This method would act as the baseline model for our case

### Evaluation
| Data | Accuracy (%) | ARI (%) | NMI (%) |
| --- | --- | --- | --- |
| Idaho Montana Tungsten |  |  |  |
| Great Basin Area Tungsten |  |  |  |

### Run Time

---

## ver. 1 (Last Updated: 01/15/2024)
### Description:
Mineral site record linking method first based on geolocations then on all other textual information available in the data

### Details:
- Generic method for forming pseudo documents
    - `{short description of attribute} is {corresponding value}`
- Use sentence transformer (mpNet) to generate textual embeddings of the pseudo documents
- Use UMAP embedding reduction to reduce textual embedding dimension from 768 to $n$
    - Value of $n$ is still being determined

### ver. 1.1 (Last Updated: )
#### Details:
- Text embedding is not reduced (768 dimension)
#### Evaluation
| Data | Accuracy (%) | ARI (%) | NMI (%) |
| --- | --- | --- | --- |
| Idaho Montana Tungsten |  |  |  |
| Great Basin Area Tungsten |  |  |  |

### ver. 1.2 (Last Updated)
#### Details:
- Text embedding is reduced to 12 dimension
    - Mainly because USMIN has only 13 data point
#### Evaluation
| Data | Accuracy (%) | ARI (%) | NMI (%) |
| --- | --- | --- | --- |
| Idaho Montana Tungsten |  |  |  |
| Great Basin Area Tungsten |  |  |  |

### Run Time
- Forming textual embedding for documents:
- Textual embedding reduction:

---

## ver. 2 (Last Updated: 01/15/2024)
### Description:
Mineral site record linking method first based on geolocations then on selected textual information available in the data

### Details:
- Identical as method described in version 1, yet only uses the following selected information (if available)
    - Name of mineral site
    - Other possible names of mineral site
    - Commodities available
    - Ore and Ganges  of mineral site

### ver. 2.1 (Last Updated: )
#### Details
- Text embedding is not reduced
#### Evaluation
| Data | Accuracy (%) | ARI (%) | NMI (%) |
| --- | --- | --- | --- |
| Idaho Montana Tungsten |  |  |  |
| Great Basin Area Tungsten |  |  |  |

### ver. 2.2 (Last Updated: )
#### Details
- Text embedding is reduced to 12 dimension
#### Evaluation
| Data | Accuracy (%) | ARI (%) | NMI (%) |
| --- | --- | --- | --- |
| Idaho Montana Tungsten |  |  |  |
| Great Basin Area Tungsten |  |  |  |

### Run Time

---

## ver. 3 (Last Updated: 01/15/2024)
### Description:
Mineral site record linking method first based on geolocations then based on mineral site names

### ver. 2.1 (Last Updated: )
#### Details
- Text embedding is not reduced
#### Evaluation
| Data | Accuracy (%) | ARI (%) | NMI (%) |
| --- | --- | --- | --- |
| Idaho Montana Tungsten |  |  |  |
| Great Basin Area Tungsten |  |  |  |

### ver. 3.2 (Last Updated: )
#### Details:
- Text embedding is reduced to 12 dimension
#### Evaluation:
| Data | Accuracy (%) | ARI (%) | NMI (%) |
| --- | --- | --- | --- |
| Idaho Montana Tungsten |  |  |  |
| Great Basin Area Tungsten |  |  |  |
### Run Time