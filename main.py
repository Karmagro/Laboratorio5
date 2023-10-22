import csv

import psycopg2

import psycopg2.extras

conn = psycopg2 . connect ( host ="cc3201.dcc.uchile.cl",
                            database ="cc3201",
                            user ="cc3201",
                            password ="j'<3_cc3201", port ="5440")

cur = conn.cursor()

def findOrInser0(name):
    cur.execute("SELECT id FROM superheroes.galvarena_character WHERE name = %s LIMIT 1", [name])
    r = cur.fetchone()
    if(r):
        return r[0]
    else:
        cur.execute("INSERT INTO superheroes.galvarena_character (name) VALUES (%s) RETURNING id", [name])
        return cur.fetchone()[0]

def findOrInser1(id, name, str, int, spd):
    cur.execute("SELECT id FROM superheroes.galvarena_superheroe WHERE id = %s LIMIT 1", [id])
    r = cur.fetchone()
    if(r):
        return r[0]
    else:
        cur.execute("INSERT INTO superheroes.galvarena_superheroe (id, name, strength, intelligence, speed) VALUES (%s,%s,%s,%s,%s) RETURNING id", [id, name,str,int,spd])
        return cur.fetchone()[0]

with open('Laboratorio_05_data.csv', encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile, delimiter=',', quotechar='"')
    i = 0
    for row in reader:
        if i == 0:
            i+=1
            continue

        id = row[0]
        super_name = row[1]
        intelligence = row[2]
        strength = row[3]
        speed = row[4]
        full_name = row[8]
        alter_egos = row[9]
        aliases_001 = row[10]
        place_of_birth = row[11]
        first_appearance = row[12]
        publisher = row[13]
        alignment = row[14]
        gender = row[15]
        race = row[16]
        height_001 = row[17]
        height_002 = row[18]
        weight_001 = row[19]
        weight_002 = row[20]
        eye_color = row[21]
        hair_color = row[22]
        occupation = row[23]
        base = row[24]
        group_affiliation = row[25]
        relatives = row[26]

        if not full_name:
            full_name = super_name
        if strength!='null':
            strength = int(row[3])
        else:
            strength = None
        if intelligence!='null':
            intelligence = int(row[2])
        else:
            intelligence = None
        if speed!='null':
            speed = int(row[4])
        else:
            speed = None

        character_id = findOrInser0(full_name)

        superheroe_id = findOrInser1(character_id,super_name,strength, intelligence, speed)

        i+= 1

conn.commit()
cur.close()
conn.close()