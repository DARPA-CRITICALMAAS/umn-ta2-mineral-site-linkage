import polars as pl

# pl_minerals = pl.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/DATA/RAW_DATA/https:__mrdata.usgs.gov_carbonatite/minerals2.csv').with_columns(
#     pl.col('value').str.to_titlecase()
# )

# nuri = pl.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/mrds_commodity_code.csv')['CommodityinMRDS'].to_list()

# pl_minerals = pl_minerals.filter(
#     pl.col('value').is_in(nuri)
# )

# pl_minerals.write_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/DATA/RAW_DATA/https:__mrdata.usgs.gov_carbonatite/minerals2.csv')

pl_main = pl.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/DATA/RAW_DATA/https:__mrdata.usgs.gov_carbonatite/main.csv').with_columns(
    commod = pl.lit("Lanthanum, Praseodymium, Samarium, Gadolinium, Thulium, Holmium, Cerium, Europium, Yttrium, Ytterbium, Erbium, Neodymium, Terbium, Dysprosium, Lutetium, Niobium")
)

pl_main.write_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/DATA/RAW_DATA/https:__mrdata.usgs.gov_carbonatite/main.csv')