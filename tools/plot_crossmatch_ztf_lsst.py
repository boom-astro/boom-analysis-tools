import io
import json
import math
import matplotlib.pyplot as plt

from astropy.time import Time
from utils.mongo import fetch_mongo

def build_ztf_photometry(data):
    processed = []
    last_jd = None
    for p in data:
        if p.get("jd") == last_jd:
            continue # Skip duplicate jd entries
        last_jd = p.get("jd")

        flux = p.get("flux")
        flux = flux / 1e9 if flux and flux > 0 else None # Convert from nJy to Jy
        flux_err = p.get("flux_err")
        flux_err = flux_err / 1e9 if flux_err and flux_err > 0 else None # Convert from nJy to Jy

        zp = p.get("zero_point", 26.0)
        new = p.copy()

        if flux:
            mag = -2.5 * math.log10(flux) + zp
            mag_err = 1.0857 * (flux_err / flux) if flux_err and flux > 0 else None
            new["mag"] = mag
            new["mag_err"] = mag_err
        elif flux_err and zp:
            new["mag_limit"] = -2.5 * math.log10(5 * flux_err) + zp

        processed.append(new)
    return processed

def build_lsst_photometry(data):
    processed = []
    last_jd = None
    for p in data:
        if p.get("jd") == last_jd:
            continue # Skip duplicate jd entries
        last_jd = p.get("jd")
        processed.append({
            "jd": p.get("jd"),
            "mag": p.get("magpsf"),
            "mag_err": p.get("sigmapsf"),
            "band": p.get("band"),
        })
    return processed

def plot_crossmatch_ztf_lsst(ztf_alert, filter):
    """
    Plot ZTF and LSST photometry for a given alert.
    Args:
        ztf_alert: ZTF alert record.
        filter: Boom filter information containing LSST ID.

    Returns:
        bytes: A BytesIO object containing the PNG image data.
    """
    lsst_id = json.loads(filter.get("annotations")).get("lsst")[0]
    lsst_aux_alert = fetch_mongo("LSST_alerts_aux").find_one({"_id": str(lsst_id)})
    if not lsst_aux_alert:
        print(f"No LSST aux alert found for _id: {lsst_id}")

    lsst_photometry = build_lsst_photometry(lsst_aux_alert.get("prv_candidates", []))
    ztf_photometry = build_ztf_photometry(ztf_alert["photometry"])

    band_colors = {
        "ztfg": "green",
        "ztfr": "red",
        "ztfi": "orange",
        "i": "brown",
        "z": "purple",
    }

    survey_markers = {
        "ZTF": "o",
        "LSST": "s",
    }

    now = Time.now().jd
    plt.figure(figsize=(8, 5))
    for p in ztf_photometry:
        plt.scatter(
            now - p["jd"],
            p.get("mag", p.get("mag_limit")),
            color=band_colors.get(p.get("band"),"black"),
            marker=survey_markers["ZTF"],
            label=f"ZTF/{p.get('band')}"
        )
    for p in lsst_photometry:
        plt.scatter(
            now - p["jd"],
            p["mag"],
            color=band_colors.get(p.get("band"),"black"),
            marker=survey_markers["LSST"],
            label=f"LSST/{p.get('band')}"
        )

    plt.gca().invert_yaxis()
    plt.gca().invert_xaxis()
    plt.xlabel("Days ago")
    plt.ylabel("Magnitude")
    plt.title(f"ZTF X LSST Photometry for {ztf_alert['objectId']}")

    # Avoid duplicate labels in legend
    handles, labels = plt.gca().get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    plt.legend(by_label.values(), by_label.keys())

    plt.grid(True)
    plt.tight_layout()

    # Save plot to BytesIO
    bytes = io.BytesIO()
    plt.savefig(bytes, format="png", bbox_inches="tight")
    plt.close()
    bytes.seek(0)
    return bytes