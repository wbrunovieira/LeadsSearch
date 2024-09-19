import psycopg2
import pandas as pd

def avaliar_qualidade():
    conn = psycopg2.connect(
        dbname="leadsdb",
        user="postgres",
        password="postgres",
        host="db"
    )
    cur = conn.cursor()

    cur.execute("SELECT id, nome, endereco, telefone, site FROM leads WHERE qualidade IS NULL")
    rows = cur.fetchall()

    for row in rows:
        lead_id = row[0]
        nome = row[1]
        endereco = row[2]
        telefone = row[3]
        site = row[4]

        # Avaliação simples: considerar um lead como 'bom' se todos os campos forem preenchidos
        campos_preenchidos = sum([bool(nome), bool(endereco), bool(telefone), bool(site)])
        qualidade = 'bom' if campos_preenchidos == 4 else 'ruim'

        # Atualizar a qualidade e campos preenchidos no banco de dados
        cur.execute(
            "UPDATE leads SET qualidade = %s, campos_preenchidos = %s WHERE id = %s",
            (qualidade, campos_preenchidos, lead_id)
        )

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    avaliar_qualidade()
