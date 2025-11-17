import concurrent.futures
import json
import math
import random
import string
import time

from time import sleep
from uuid import uuid4
from datetime import datetime

from faker import Faker
from kafka import KafkaProducer
from pyspark.sql.connect.functions import to_date
from pyspark.sql.utils import to_str

#le but est de savoir tout ce qui se passe lors d'un trajet d'une ville A vera une ville B
#une ville est identifié par son ID, son nom, ses coordonnées géographiques


list_ville = [
    {"name": "Domicile", "latitude": "46.851863", "longitude": "-71.207157"},
    {"name": "Bureau", "latitude": "46.797777", "longitude": "-71.263931"},
    {"name": "Orsainville", "latitude": "46.883835", "longitude": "-71.290048"},
    {"name": "Saint-Hyacinthe", "latitude": "45.615659", "longitude": "-72.967415"}
]



def generer_identifiant_voyage():
    maintenant = datetime.now()
    date_str = maintenant.strftime("%Y%m%d%H%M%S")
    numero = random.randint(10000, 99999)
    return f"TR{date_str}-{numero}"



def random_trip(list_ville):

    departure = random.choice(list_ville)
    while True:
        arrival = random.choice(list_ville)
        if departure != arrival:
            break

    start_time = datetime.now()
    return {
        "tripId":generer_identifiant_voyage(),
        "departure":departure,
        "arrival":arrival,
        "heure_depart":start_time,
        "flag_arrivee": False
    }

def driver(permisConduire, nameChauffeur):
    return {
        "permis":permisConduire,
        "nom": nameChauffeur
    }

def vehicule_data_generators(trajet_id:dict, driver:dict,immatriculation, marque, vin):
    #faker = Faker()
    return {
        "immatriculation":immatriculation,
        "vin": vin,
        "marque": marque,
        "année": random.randint(2010, 2024),
        "color": random.choice(['BLANCHE','NOIRE','GRISE','VERTE','BLEU','ROUGE','JAUNE']),
        "fuelType": random.choice(['Essence','Gasoil','Electric','Hybrid']),
        "IDTrajet": trajet_id['tripId'],
        "IDChauffeur": driver['permis'],
        "speed":0.0,
        "DateDebutTrajet": trajet_id['heure_depart']
    }

# def simuler_deplacement(trajet:dict):
#     #pas de deplacement
#     pas = 0.0001
#     print(f"Début du trajet {trajet['tripId']} ")
#     depart=trajet['departure']
#     arrival=trajet['arrival']
#
#
#     delta_lat=(float(arrival['latitude']) - float(depart['latitude']))
#     delta_long=(float(arrival['longitude']) - float(depart['longitude']))
#
#     inc_lat = pas if delta_lat > 0 else -pas
#     inc_long = pas if delta_long > 0 else -pas
#
#
#     trajet['departure']['latitude'] = str(inc_lat + float(trajet['departure']['latitude']))
#     trajet['departure']['longitude'] = str(inc_long + float(trajet['departure']['longitude']))
#     #trajet['departure']['latitude']  = random.uniform(-0.0005, 0.0005) + float(trajet['departure']['latitude'])
#     #trajet['departure']['longitude'] = random.uniform(-0.0005, 0.0005) + float(trajet['departure']['longitude'])
#     trajet['heure_depart']=datetime.now()
#
#     if ((float(trajet['departure']['latitude']) >= float(trajet['arrival']['latitude']) and inc_lat > 0)
#             or (float(trajet['departure']['latitude']) <= float(trajet['arrival']['latitude']) and inc_lat <0)):
#         trajet['departure']['latitude'] = trajet['arrival']['latitude']
#
#     if ((float(trajet['departure']['longitude']) >= float(trajet['arrival']['longitude']) and inc_long > 0)
#             or (float(trajet['departure']['longitude']) <= float(trajet['arrival']['longitude']) and inc_long < 0)):
#         trajet['departure']['longitude'] = trajet['arrival']['longitude']
#
#     if ((float(trajet['departure']['latitude'])) == (float(trajet['arrival']['latitude']))
#             and (float(trajet['departure']['longitude'])) == (float(trajet['arrival']['longitude']))):
#         print("Nous sommes à destination")
#
#
#     print(f"Fin du trajet {trajet['tripId']}")
#
#     return {
#         "tripId": trajet['tripId'],
#         "departure": trajet['departure'],
#         "arrival": trajet['arrival'],
#         "heure_depart": trajet['heure_depart']
#     }

#fonction qui renvoit la distance entre distance 2 points en km
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0  # rayon Terre en km
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c  # distance en km

def simuler_deplacement1(voiture:dict,trajet:dict):

    #seuil d'arret, quand le vehicule est très proche du point d'arrivée
    distance_residuelle = 0.0005 #km


    print(f"****************************************Début du trajet {trajet['tripId']} *****************************************************************")

    depart=trajet['departure']
    arrival = trajet['arrival']

    lat1=float(depart['latitude'])
    lon1=float(depart['longitude'])
    lat2=float(arrival['latitude'])
    lon2=float(arrival['longitude'])

    distance_restante = haversine(lat1, lon1, lat2, lon2)
    print(f"************{distance_restante}*************")
    vites = str(random.randint(20, 90))
    voiture['speed'] = vites

    if distance_restante > distance_residuelle:
        distance_par_seconde = float(vites) / 3600

        # Calcul du ratio de progression
        ratio = distance_par_seconde / distance_restante

        # calcul de la distance restante entre les deux points
        delta_lat = (float(arrival['latitude']) - float(depart['latitude']))
        delta_long = (float(arrival['longitude']) - float(depart['longitude']))

        #Progression
        progression_lat = delta_lat * ratio
        progression_lon = delta_long * ratio


        #Avancement
        trajet['departure']['latitude'] = str(progression_lat + float(trajet['departure']['latitude']))
        trajet['departure']['longitude'] = str(progression_lon + float(trajet['departure']['longitude']))
        trajet['heure_depart'] = datetime.now()
    else:
        print("Nous sommes à destination")
        trajet['flag_arrivee']=True


    print(f"Fin du trajet {trajet['tripId']}")

    return {
        "tripId": trajet['tripId'],
        "departure": trajet['departure'],
        "arrival": trajet['arrival'],
        "heure_depart": trajet['heure_depart'],
        "flag_arrivee": trajet['flag_arrivee']
    }


def generate_weather_data(vehicle:dict, traje_id:dict):

    return {
        "WeatherTime": vehicle['DateDebutTrajet'],
        "wvehicleID": vehicle['immatriculation'],
        "weatherTrip": traje_id['tripId'],
        "weatherCondition": random.choice(['RAIN','SNOW','CLOUDY']),
        "temperature": random.randint(-30, 30),
        "wind_speed": random.randint(0, 50),
        "wind_dir": random.choice(['W','S','O','E']),
        "wind_degree": random.randint(0, 60),
        "humidity": random.randint(0, 100)
    }


def generate_emergency_data(vehicle:dict, traje_id:dict):

    emergency=random.choice(['ACCIDENT','FIRE','POLICE','MEDICAL','NONE'])
    status=random.choice(['RESOLVED', 'ON GOING'])

    if emergency=='NONE':
        status='NONE'

    return {
        "EmergencyTime": vehicle['DateDebutTrajet'],
        "evehicleID": vehicle['immatriculation'],
        "emergencyTrip": traje_id['tripId'],
        "emergencyType": emergency,
        "emergencyStatus": status

    }

def serialize_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

def retrait_obj_list(l:list, obj):
    l.remove(obj)

def main():
    faker = Faker()
    producer=KafkaProducer(bootstrap_servers=['kafka:9092'],
                           value_serializer=lambda v: json.dumps(v, default=serialize_datetime).encode('utf-8'))



    # liste statique de 30 chauffeurs de l'entreprise, un chauffeur effectue un et un seul trajet à un moment donné t
    permisConduire = [''.join(random.choices(string.ascii_uppercase + string.digits, k=8)) for _ in range(14)]
    nameChauffeur = [faker.name() for _ in range(14)]

    #liste des vehicules du parc auto
    marque = ['BMW', 'TOYOTA', 'HONDA', 'HYUNDAI', 'CHEVROLET', 'FORD', 'MERCEDES','CHRYSLER','AUDI','MINI-COOPER','RANGE-ROVER','MITSUBISHU', 'FORD-ESCAPE','POINTIAC']
    immatriculations = [ ''.join(random.choices(string.ascii_uppercase + string.digits, k=6)) for _ in range(len(marque))]
    NIV= [faker.vin() for _ in range(len(marque))]

    #liste des trajets en cours
    trajets = []

    #liste des vehicules en cours
    cars = []

    ongoing_driver = []
    ongoing_car = []
    ongoing_model = []
    ongoing_driver_name = []

    start = time.time()

    startapp=True

    #liste des topics
    #trajet_data,weather_data,emergency_data,vehicle_data

    while True:
        end = start + 900
        while start <= end:

            #génération d'un voyage
            t=random_trip(list_ville)
            trajets.append(t)
            print(trajets)

            #ajout du trajet dans un topic
            producer.send('trajet_data1',t)

            #recuprere le dernier element, de la liste car il reprsentara le dernier trajet oour lequel aucun vehicule ne lui ait assigné
            trajet = trajets[-1]
            print(trajet)


            #choix de la voiture et du chauffeur
            immat_choisi = random.choice(immatriculations)
            model_choisi=marque[immatriculations.index(immat_choisi)]
            chauffeur_choisi=random.choice(nameChauffeur)
            permisDeConduire=permisConduire[nameChauffeur.index(chauffeur_choisi)]
            print("************Affichage des vehicules disponibles******")
            print(f"Marque: {marque}")
            print(f"Immat: {immatriculations}")
            print(f"Chauffeur :{nameChauffeur}")
            print(f"Permis :{permisConduire}")

            car = vehicule_data_generators(trajet, driver(permisDeConduire, chauffeur_choisi), immat_choisi, model_choisi, NIV[immatriculations.index(immat_choisi)])
            cars.append(car)
            #ajout du car dans le topic car
            print(car)
            producer.send('vehicle_data1', car)
            print(car)
            print("************Affichage des vehicules en cours de déplacement******")
            #Ajout du chauffeur et de la voiture choisie dans la liste en cours de voyage
            ongoing_driver.append(car['IDChauffeur'])
            ongoing_driver_name.append(nameChauffeur[permisConduire.index(car['IDChauffeur'])])
            ongoing_car.append(car['immatriculation'])
            ongoing_model.append(car['marque'])
            print(f"Driver: {ongoing_driver}")
            print(f"Car : {ongoing_car}")
            print(f"Modele : {ongoing_model}")

            #retrait des vehicules et chauffeurs choisis dans la liste des personnes disponibles pour nouveau trajets
            print("************Affichage des vehicules disponibles apres un trajet******")
            #marque.remove(car['marque'])
            # NIV.remove(car['vin'])
            # immatriculations.remove(car['immatriculation'])
            # nameChauffeur.remove(nameChauffeur[permisConduire.index(car['IDChauffeur'])])
            # permisConduire.remove(car['IDChauffeur'])
            retrait_obj_list(marque,car['marque'])
            retrait_obj_list(NIV, car['vin'])
            retrait_obj_list(immatriculations, car['immatriculation'])
            retrait_obj_list(nameChauffeur, nameChauffeur[permisConduire.index(car['IDChauffeur'])])
            retrait_obj_list(permisConduire, car['IDChauffeur'])


            print(f"Marque: {marque}")
            print(f"Immat: {immatriculations}")
            print(f"Chauffeur :{nameChauffeur}")
            print(f"Permis :{permisConduire}")

            #Assocation conditions metéorologiques sur un trajet x
            #Apres chaque 5 minutes, génère pour chaque trajet, des conditions metéo sur ce trajet
            #generate_weather_data(car, t)
            print("***********génération des données méteoroliques***************")
            with concurrent.futures.ThreadPoolExecutor() as executor:
                weathers= list(executor.map(generate_weather_data, cars,trajets))
            print(weathers)
            #ajout des donnees meteo dans le topic weather
            for weath in weathers:
                producer.send('weather_data1', weath)


            print("***********génération des données cas d'urgence eventuelles sur le trajet***************")
            with concurrent.futures.ThreadPoolExecutor() as executor:
                emergency= list(executor.map(generate_emergency_data, cars,trajets))
            print(emergency)

            #ajout des donnees durgence dans ls topic urgence
            for emer in emergency:
                producer.send('emergency_data1', emer)



            #simulation du déplacement
            while startapp and start <= end:
                #si trajets est vide alors il y'a aucun trajet pour l'instant, patientez 5minutes
                if len(trajets)!=0:


                    for t in trajets:
                        # si la condition est vrai alors on est arrivée à destination, et on retire le trajet des la liste des trajets, le chauffeur et la voiture des en cours de voyage
                        # et on les rajoute dans la liste des vehicules et chauffeurs disponibles pur un nouveau depart
                        if t['flag_arrivee']:
                        #if float(t['departure']['latitude']) == float(t['arrival']['latitude']) and float(t['departure']['longitude']) == float(t['arrival']['longitude']):
                            print("voiture arrivé à destination")
                            #trajets.remove(t)
                            retrait_obj_list(trajets,t)
                            #on recherche la voiture associée au trajet afin de la retirer de la liste des voitures en cours et de les rendre disponibles à nouveau
                            for voiture in cars:
                                if str(voiture['IDTrajet']) == str(t['tripId']):
                                    #rajout dans la liste des disponibles

                                    marque.append(voiture['marque'])
                                    NIV.append(voiture['vin'])
                                    immatriculations.append(voiture['immatriculation'])
                                    permisConduire.append(voiture['IDChauffeur'])
                                    nameChauffeur.append(ongoing_driver_name[ongoing_driver.index(voiture['IDChauffeur'])])


                                    #retrait des en cours de voyage
                                    ongoing_car.remove(voiture['immatriculation'])
                                    ongoing_model.remove(voiture['marque'])
                                    ongoing_driver_name.remove(ongoing_driver_name[ongoing_driver.index(voiture['IDChauffeur'])])
                                    ongoing_driver.remove(voiture['IDChauffeur'])

                                    cars.remove(voiture)

                                    print(" ils sont rendus de nouveau disponible apres leur trajet")
                                    print(f"Marque: {marque}")
                                    print(f"Immat: {immatriculations}")
                                    print(f"Chauffeur :{nameChauffeur}")
                                    print(f"Permis :{permisConduire}")




                    #je simule le deplacement et je break de la bouche startapp si j'ai deja fait 1h de tmps afin de generer un nouveau trajet
                    print("Simulation du deplacement")
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        deplacement = list(executor.map(simuler_deplacement1, cars,trajets))
                    print("Fin Simulation du deplacement")

                    #ajout du trajet dans le topic  trajet
                    for dep in deplacement:
                        print("******************************Ecriture dans le topic tajet***********************")
                        producer.send('trajet_data1', dep)
                    sleep(1)
                    print(trajets)
                    print(cars)
                    start = time.time()
                else :
                    print("Pas voiture en trajet actuellement.... attente de nouveau trajet")
                    start = time.time()
                    print(f" Actual time {start}")
                    print(f" Wait until {end} to trigger a new trip")
                    sleep(1)


        start = time.time()

    # faker.name()
    # depart=random.choice(list_ville)
    # print(type(depart))
    # print(faker.random_number())
    # print(random_trip(list_ville))
    # print("")
    # travel=random_trip(list_ville)
    # print(''.join(random.choices(string.ascii_uppercase+" "+string.digits,k=7)))
    # print(travel['departure']['name'])
    # print(random.randint(2015, 2024))
    # a=vehicule_data_generators(travel,driver(permisConduire,nameChauffeur),immatriculations,marque,NIV)
    # print(a)
    # print(travel)
    # b=simuler_deplacement(travel)
    # print(b)
    # c = simuler_deplacement(travel)
    # print(c)
    # d=generate_weather_data(a,travel)
    # print(d)
    # e=generate_emergency_data(a,travel)
    # print(e)


if __name__ == '__main__':
    main()