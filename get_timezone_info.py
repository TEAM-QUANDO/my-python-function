from timezonefinder import TimezoneFinder

t_find = TimezoneFinder()
# Provide the latitude
latitude = 54.743
# Provide the longitude
longitude = 15.492
timezone_info = t_find.timezone_at(lng=longitude, lat=latitude)
print(timezone_info)