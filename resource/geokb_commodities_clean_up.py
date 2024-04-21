import polars as pl

pl_geokb = pl.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/geokb_commodities.csv')
pl_geokb = pl_geokb.with_columns(
    pl.col('item').str.split('/entity/').list.get(1)
)

pl_current_minmod = pl.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/minmod_commodities.csv')
pl_minmod_list = pl_current_minmod['IDinGeoKB'].to_list()

pl_geokb = pl_geokb.filter(
    ~pl.col('item').is_in(pl_minmod_list)
).with_columns(
    pl.col('itemLabel').str.to_lowercase()
)

geokb_remaining_list = pl_geokb['itemLabel'].to_list()


pl_mrds_codes = pl.read_csv('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/MRDS_commodity_code.csv')
pl_mrds_codes = pl_mrds_codes.with_columns(
    CommodityinMRDS = pl.col('Commodity name'),
).with_columns(
    pl.col('Commodity name').str.to_lowercase()
).filter(
    pl.col('Commodity name').is_in(geokb_remaining_list)
).rename({
    'Commodity name': 'itemLabel',
    'Code': 'CodeinMRDS'
})

pl_new = pl.concat(
    [pl_mrds_codes, pl_geokb],
    how='align'
).with_columns(
    minmod_id = pl.lit('Q') + pl.Series(list(range(619, 619+26))).cast(pl.Utf8)
).select(
    minmod_id=pl.col('minmod_id'),
    CommodityinMRDS=pl.col('CommodityinMRDS'),
    CommodityinGeoKb=pl.col('itemLabel'),
    IDinGeoKB=pl.col('item'),
)

pl_new.write_csv('./additional.csv')

print(pl_new)
