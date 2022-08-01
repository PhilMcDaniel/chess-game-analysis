import requests

#Get real-time users status from Lichess
# doesn't require authentication
base_url = 'https://lichess.org/api/users/status'
#ids from the MNCS chess league
ids_list = ['amronyoutube','calster','ctnivlac','ekul3333','ewsteener','grapechessplayer','isodor','okayengineer','pcmcd','maxprovolt','saberstreamz','slipperyj','crustytammy','bradistrash','webspace','wastee2']

#format ids into comma separated list for api query param
ids_string = ''
for id in ids_list:
    ids_string = ids_string+id+','

#remove the trailing ','
ids_string = ids_string[:-1]

combined_url = base_url + f'?ids={ids_string}'
combined_url
response = requests.get(url = combined_url)
print(response.status_code)

response.json()