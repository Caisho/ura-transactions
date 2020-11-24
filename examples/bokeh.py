import os
from dotenv import load_dotenv
from bokeh.models import ColumnDataSource, GMapOptions
from bokeh.plotting import gmap, output_file, figure, show

load_dotenv()
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')

output_file("gmap.html")

map_options = GMapOptions(lat=1.35, lng=103.84, map_type="roadmap", zoom=11)

# For GMaps to function, Google requires you obtain and enable an API key:
#
#     https://developers.google.com/maps/documentation/javascript/get-api-key
#
# Replace the value below with your personal API key:
p = gmap(GOOGLE_API_KEY, map_options, title="Singapore")

source = ColumnDataSource(
    data=dict(lat=[1.2989414989161459],
              lon=[103.76392214279238])
)

p.circle(x="lon", y="lat", size=10, fill_color="blue", fill_alpha=0.8, source=source)

show(p)
