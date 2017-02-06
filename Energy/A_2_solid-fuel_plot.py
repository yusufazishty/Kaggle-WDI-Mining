# -*- coding: utf-8 -*-
"""
Created on Fri Apr 22 00:26:29 2016

@author: yusufazishty
"""

import plotly
import pandas as pd

DATA_PATH = r"E:\UserTA\5112100086\Dropbox\[PENTING TIDAK URGENT]\[ARSIP KULIAH]\SEMESTER 8\Kuliah\Big Data\BigData4\Energy\clastered_data\[B]_2_fuel_clustered.txt"
OUTPUT_PLOTED_FILE=r"clastered_data\[B]_2_fuel_ploted"
OUTPUT_CLUSTERED_FILE=r"clastered_data\[B]_2_fuel_clustered"

df = pd.read_csv(DATA_PATH, delimiter=";")

data = [ dict(
        type = 'choropleth',
        locations = df['CountryCode'],
        z = df['AvgFuel'],
        text = df['CountryName'],
        colorscale = [[0,"rgb(5, 10, 172)"],[0.35,"rgb(40, 60, 190)"],[0.5,"rgb(70, 100, 245)"],\
            [0.6,"rgb(90, 120, 245)"],[0.7,"rgb(106, 137, 247)"],[1,"rgb(220, 220, 220)"]],
        autocolorscale = False,
        reversescale = True,
        marker = dict(
            line = dict (
                color = 'rgb(180,180,180)',
                width = 0.5
            )
        ),
        colorbar = dict(
            autotick = False,
            tickprefix = '%',
            title = 'Population Get Access to Fuel in %'
        ),
    ) ]
    
data2 = [ dict(
        type = 'choropleth',
        locations = df['CountryCode'],
        z = df['Cluster'],
        text = df['CountryName'],
        colorscale = [[0,"rgb(5, 10, 172)"],[0.5,"rgb(70, 100, 245)"],[0.7,"rgb(106, 137, 247)"],[1,"rgb(220, 220, 220)"]],
        autocolorscale = False,
        reversescale = True,
        marker = dict(
            line = dict (
                color = 'rgb(180,180,180)',
                width = 0.5
            )
        ),
        colorbar = dict(
            autotick = False,
            tickprefix = 'class',
            title = 'Population Get Access to Fuel in Group '
        ),
    ) ]

layout = dict(
    title = 'Global Cluster of Country in Fuel Access <br>Source: \
<a href="https://www.kaggle.com/worldbank/world-development-indicators">\
World Development Indicators</a>',
    geo = dict(
        showframe = False,
        showcoastlines = False,
        projection = dict(
            type = 'Mercator'
        )
    )
)

fig_plot = dict( data=data, layout=layout )
fig_cluster = dict( data=data2, layout=layout )
plotly.offline.plot( fig_plot, validate=False, filename=OUTPUT_PLOTED_FILE )
plotly.offline.plot( fig_cluster, validate=False, filename=OUTPUT_CLUSTERED_FILE )

#halo.savefig(OUTPUT_MAPE_IMAGE, bbox_inches='tight', pad_inches=.2)    