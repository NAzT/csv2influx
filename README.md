# csv2line

## Installation

```
pip install git+git://github.com/nazt/csv2line.git#egg=csv2line
```


## Usage
```
csv2line convert --csv-dir /Users/nat/Desktop/ABC/csv3/db/measurement/2019-08 --output-dir /tmp
```

```
/Users/nat/.virtualenvs/nn/bin/csv2line  convert --csv-file=
for i in $(find scripts -name \*.sh); do echo "$i"; bash "$i"; done
for i in $(find csv/ -name \*.csv); do echo "$i"; /Users/nat/.virtualenvs/nn/bin/csv2line  convert "--csv-file=$i"; done
# cat line-protocols/meta.txt /Volumes/Untitled\ 2/X/until-2020-05-02.txt  > until-2020-05-02.line

for i in $(find csv/ -name \*.csv); do echo "$i"; csv2line  convert --csv-file="$i"; done
```



pip3 install .;csv2line convert --csv-file='/Users/nat/Downloads/csv/AddLiquidityDayDatas/2021-07-27.csv' --field-time=date  --string-fields=swapid,token0,token1,token2,token3 --measurement=v1 --force True

