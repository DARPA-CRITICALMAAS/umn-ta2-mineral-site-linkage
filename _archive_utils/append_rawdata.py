import os
import time
import logging
import configparser

from utils.load_files import *
from utils.dataframe_operations import *
from utils.compare_text import *

config = configparser.ConfigParser()
config.read('./params.ini')
path_params = config['directory.paths']

def append_rawdata(pl_data):
    path_CDR = path_params['URL_CDR_DATA']

    dict_raw_data_sources = load_directory_names(path_CDR)
    list_queried_sources = pl_data.unique('source_id')['source_id'].to_list()

    for source in list_queried_sources:
        subbed_source_name = re.sub('[^A-Za-z0-9]', '', source).lower()

        if subbed_source_name in list(dict_raw_data_sources.keys()):
            path_identified_rawdata = dict_raw_data_sources[subbed_source_name]

            data_dictionary = initiate_load(os.path.join(path_identified_rawdata, 'dictionary.csv'))
            data_dictionary = string_match_on_attribute(data_dictionary, 'Other Name')

            pl_raw_data = load_directory(path_identified_rawdata, list_exclude_filename=['dictionary']).with_columns(
                source_id = pl.lit(source)
            )

            columns_raw_data = list(pl_raw_data.columns)

            identified_attribute_label = None
            try:
                identified_attribute_label = data_dictionary.with_columns(
                    identified_labels = pl.struct(pl.all()).map_elements(lambda x: identify_list_items_overlap(x, columns_raw_data))
                ).item(0, 'identified_labels')
            except:
                pass

            if identified_attribute_label:
                list_record_id = initiate_load(os.path.join(path_params['PATH_RSRC_DIR'], 'attribute_archive.pkl'))['record_id']
                attribute_record_id = list(set(list_record_id) & set(columns_raw_data))[0]

                pl_raw_data = pl_raw_data.group_by(
                    attribute_record_id
                ).agg(
                    [pl.all().cast(pl.Utf8)]
                ).with_columns(
                    pl.exclude(attribute_record_id).list.unique().list.drop_nulls().list.join(",")
                )

                try:
                    pl_raw_data = pl_raw_data.select(
                        pl.col(['source_id', attribute_record_id, identified_attribute_label])
                    ).rename({
                        identified_attribute_label: 'other_name',
                        attribute_record_id: 'record_id',
                    }).with_columns(
                        pl.col('record_id').cast(pl.Utf8)
                    )

                except:
                    pass
                
                pl_data = pl.concat(
                    [pl_data, pl_raw_data],
                    how='align'
                ).drop_nulls(
                    subset=['record_id']
                ).fill_null('')


    pl_data = pl_data.rename(
        {'site_name': 'name'}
    ).with_columns(
        pl.concat_str(
            [
                pl.col(['name', 'other_name']),
            ],
            separator=",",
        ).alias("site_name"),
    ).drop(['name', 'other_name']).with_columns(
        pl.col('site_name').str.replace(r'<Null>', '')
    )

    pl_data = split_str_column(pl_data, 'site_name', bool_replace_numbers=True).explode(
        'site_name'
    ).with_columns(
        pl.col('site_name').str.strip_chars()
    ).filter(
        (pl.col('site_name') != '') & (pl.col('site_name') != 'Null')
    )

    return pl_data