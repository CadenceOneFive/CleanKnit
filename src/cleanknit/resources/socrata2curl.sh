# Download list of Socrata domains nd post-process the result

# Post process the JSON from Socrata metadata API call e.g. curl --silent 'http://api.us.socrata.com/api/catalog/v1/domains'
# The processing  generates curl commands that can be used to download the full resource descriptions
# for that domain.

# jq 7.73 has some new options for creating directories
# https://daniel.haxx.se/blog/2020/09/10/store-the-curl-output-over-there/
#
# However my version is on 7.68 so we hack an equivalent via codegen
#
# the intent is to organize data and metadata for a domain underneath a directory of the same name
export socrata_output_dir="/mnt/c/data/socrata"
jq --raw-output '.results[] | select(.count > 0)| "(mkdir -p " + env.socrata_output_dir + "/" + .domain + "; cd " + env.socrata_output_dir + " ; curl \"https://api.us.socrata.com/api/catalog/v1?domains=" + .domain+"&offset=0&limit="+(.count|tostring) + "\" --output " + .domain +"/" + .domain + ".json" +" )"'



# select length(readfile(d.name)),*
# FROM lsdir('.') as d
# where regexp_like(d.name, '[a-z0-9]+.[a-z0-9]+.json$')
# and length(d.name) <> 9
# and not (d.name like '%metadata%') ;