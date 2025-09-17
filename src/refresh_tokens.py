from datetime import datetime, timedelta
from refresh_facebook import renovar_facebook
# (aquí importas también renovar_linkedin, renovar_tiktok, etc.)

from tokens import obtener_token

def renovar_todos():
    ahora = datetime.now()

    # Facebook
    fb = obtener_token("facebook")
    if fb["expira_en"] and fb["expira_en"] < ahora + timedelta(days=5):
        renovar_facebook()

    # LinkedIn
    # li = obtener_token("linkedin")
    # if li["expira_en"] and li["expira_en"] < ahora + timedelta(minutes=10):
    #     renovar_linkedin()

    # TikTok
    # tk = obtener_token("tiktok")
    # if tk["expira_en"] and tk["expira_en"] < ahora + timedelta(days=2):
    #     renovar_tiktok()

    # Twitter
    # tw = obtener_token("twitter")
    # if tw["expira_en"] and tw["expira_en"] < ahora + timedelta(minutes=10):
    #     renovar_twitter()

if __name__ == "__main__":
    renovar_todos()
