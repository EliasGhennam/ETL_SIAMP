def get_ecb_rates(date: str | None = None, required_currencies: set[str] | None = None):
    print(f"[DEBUG] Appel get_ecb_rates(date={date})", flush=True)
    if date:
        url = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.xml"
    else:
        url = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"

    try:
        response = requests.get(url)
        print(f"[INFO] 📡 Requête vers {url}", flush=True)
        print(f"[INFO] ✅ Statut : {response.status_code}", flush=True)
        response.raise_for_status()

        root = ET.fromstring(response.content)
        ns = {'ns': 'http://www.ecb.int/vocabulary/2002-08-01/eurofxref'}

        rates = {"EUR": 1.0}
        from datetime import datetime, timedelta

        if date:
            limit_date = (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=60)).strftime("%Y-%m-%d")
            print(f"[INFO] 🔍 Recherche limitée aux taux entre {limit_date} et {date}", flush=True)



        dates = [cube.attrib["time"] for cube in root.findall(".//ns:Cube[@time]", ns)]
        if date:
            dates = sorted([d for d in dates if limit_date <= d <= date], reverse=True)
        else:
            dates = sorted(dates, reverse=True)


        rates_found = set(rates.keys())
        target_cube = None

        for d in dates:
            cube_d = root.find(f".//ns:Cube[@time='{d}']", ns)
            if cube_d is None:
                continue

            for cube in cube_d.findall("ns:Cube", ns):
                cur = cube.attrib["currency"]
                if cur not in rates:
                    rate = float(cube.attrib["rate"])
                    rates[cur] = rate
                    print(f"[INFO] ➕ Taux récupéré pour {cur} au {d} = {rate}", flush=True)
                    rates_found.add(cur)

            if required_currencies and required_currencies <= rates_found:
                print(f"[INFO] ✅ Tous les taux requis trouvés avant {d}", flush=True)
                break


        if date:
            # chercher le jour exact OU le plus proche avant
            dates = [cube.attrib["time"] for cube in root.findall(".//ns:Cube[@time]", ns)]
            print(f"[INFO] 📅 {len(dates)} dates trouvées dans l'historique ECB", flush=True)
            print(f"[INFO] 📅 Premières dates disponibles : {dates[:5]}", flush=True)
            dates.sort(reverse=True)
            target_date = None
            for d in dates:
                if d <= date:
                    target_date = d
                    break

            if not target_date:
                raise ValueError(f"Aucun taux trouvé avant la date {date}")

            target_cube = root.find(f".//ns:Cube[@time='{target_date}']", ns)
            if target_date != date:
                print(f"[INFO] ⚠️ Pas de taux pour {date}, utilisation de {target_date} à la place", flush=True)
            else:
                print(f"[INFO] ✅ Taux trouvés pour la date exacte : {target_date}", flush=True)


            if target_date != date:
                print(f"[INFO] ⚠ Aucun taux pour {date}, substitution par {target_date}", flush=True)
        else:
            # date non spécifiée : dernier taux connu
            cubes = root.findall(".//Cube[@time]")
            if not cubes:
                raise ValueError("Pas de données de taux trouvées")
            target_cube = cubes[0]
            target_date = target_cube.attrib["time"]
        
        print("[INFO] 🔎 Récupération des taux de conversion :", flush=True)
        for cube in target_cube.findall("ns:Cube", ns):
            currency = cube.attrib["currency"]
            raw_rate = float(cube.attrib["rate"])
            print(f"  → {currency} = {raw_rate}", flush=True)
            if raw_rate != 0:
                rates[currency] = raw_rate
        rates["EUR"] = 1.0

        if required_currencies:
            missing = required_currencies - rates_found
            if missing:
                print(f"[WARN] ❌ Aucun taux trouvé pour {sorted(missing)} dans les 60 derniers jours.", flush=True)
                print(f"[SUGGESTION] ✍️ Veuillez les ajouter manuellement dans l'interface ou en ligne de commande.", flush=True)



        print(f"[INFO] Taux ECB récupérés au {date}", flush=True)
        for k, v in rates.items():
            print(f"  → {k} = {v}")
        return rates

    except Exception as e:
        print(f"[ERROR] Erreur récupération ECB : {e}", flush=True)
        print("[FALLBACK] 🛑 Repli sur taux locaux codés en dur", flush=True)
        return {
        "EUR":1.0, "USD":0.93, "GBP":1.15,
        "EGP":0.03, "CHF":1.04, "AED":0.25, "JPY":0.0062
    }