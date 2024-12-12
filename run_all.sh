#!/bin/bash 


github_branch="commodity_sameas_$(date '+%Y%m%d%H%m%s')"
for i in aluminum antimony arsenic bismuth chromium cobalt copper graphite hafnium helium indium lithium manganese molybdenum nickel niobium potassium tin uranium zinc beryllium cesium gallium germanium platinum rhenium rubidium scandium strontium tantalum tellurium titanium tungsten vanadium zirconium lanthanum cerium praseodymium yttrium neodymium samarium europium gadolinium terbium dysprosium holmium erbium thulium ytterbium lutetium promethium magnesium
do
'/home/yaoyi/pyo00005/CriticalMAAS/ta2-minmod-data/data/same-as/umn/${i}'
python3 fusemine.py --commodity $i 


done