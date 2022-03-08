# Download list of Socrata domains and post-process the result
# to generate curl commands that can be used to download the full resource descriptions
# for that domain.
curl --silent 'http://api.us.socrata.com/api/catalog/v1/domains' | \
jq --raw-output '.results[] | select(.count > 0)| "curl \"https://api.us.socrata.com/api/catalog/v1?domains=" + .domain+"&offset=0&limit="+(.count|tostring) + "\" --output " + .domain + ".json"  '