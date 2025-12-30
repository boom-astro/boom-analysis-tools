import os
import argparse
import warnings
import dustmaps.sfd
import pandas as pd
import numpy as np
import astropy.units as u
import matplotlib.pyplot as plt

from contextlib import redirect_stdout, redirect_stderr
from astropy.coordinates import SkyCoord
from superphot_plus.samplers.numpyro_sampler import SVISampler
from superphot_plus.priors import SuperphotPrior
from superphot_plus.model import SuperphotLightGBM
from snapi import Photometry, Formatter
from sklearn.exceptions import InconsistentVersionWarning
from utils.mongo import fetch_mongo

dustmaps.sfd.fetch()

warnings.simplefilter("ignore", category=InconsistentVersionWarning)

MODELS_PATH = "data/models/"

def process_photometry(cand_info, source):
    """
    Process photometry data from candidate information into a pandas DataFrame.

    Extracts photometric measurements from both alert candidates and forced photometry,
    converting Julian dates to Modified Julian Dates (MJD) and organizing the data
    into a structured format.

    Args:
        cand_info (dict): Dictionary containing candidate information with keys:
            - 'prv_candidates': List of previous candidate detections
            - 'fp_hists': List of forced photometry measurements
        source (str): Source identifier (e.g., 'ZTF', 'LSST') to label the data.

    Returns:
        pd.DataFrame: DataFrame with columns:
            - mjd: Modified Julian Date
            - mag: Magnitude
            - mag_err: Magnitude error
            - filter: Filter/band name
            - type: Type of photometry ('alert' or 'forced_photometry')
            - source: Source identifier
    """
    candidates = cand_info["prv_candidates"]
    forced_photometry = cand_info["fp_hists"]
    data_dict_list = []
    
    # Process alert candidates
    for obj in candidates:
        temp_dict = {
            "mjd": obj["jd"] - 2400000.5,
            "mag": obj["magpsf"],
            "mag_err": obj["sigmapsf"],
            "filter": obj["band"],
            "type": "alert",
            "source": source
        }
        data_dict_list.append(temp_dict)

    # Process forced photometry
    for obj in forced_photometry:
        if "magpsf" in obj:
            temp_dict = {
                "mjd": obj["jd"] - 2400000.5,
                "mag": obj["magpsf"],
                "mag_err": obj["sigmapsf"],
                "filter": obj["band"],
                "type": "forced_photometry",
                "source": source
            }
            data_dict_list.append(temp_dict)

    return pd.DataFrame.from_dict(data_dict_list)


def evaluate_cal_probs(model, orig_features):
    """
    Evaluate classification probabilities using a calibrated LightGBM model.

    This function normalizes input features, predicts class probabilities, and
    calculates the most likely class and its probability using a frequentist
    approach (counting best classes across fits).

    Args:
        model (SuperphotLightGBM): Pre-trained Superphot LightGBM classification model.
        orig_features (pd.DataFrame): DataFrame containing unnormalized feature values.

    Returns:
        tuple: A tuple containing:
            - str: Best predicted supernova class
            - float: Probability of the best class
            - float: Probability of SN Ia classification (0.0 if not predicted)
    """
    allowed_types = ['SLSN-I', 'SN Ia', 'SN Ibc', 'SN II', 'SN IIn']
    input_features = model.best_model.feature_name_
    test_features = model.normalize(orig_features[input_features])
    
    probabilities = pd.DataFrame(
        model.best_model.predict_proba(test_features),
        index=test_features.index
    )
    
    probabilities.columns = np.sort(allowed_types)
    best_classes = probabilities.idxmax(axis=1)
    
    # Calculate probability as fraction of fits where each class is best
    # (frequentist interpretation for better calibration)
    probs = best_classes.value_counts() / best_classes.count()
    
    try:
        ia_prob = probs[probs.index == "SN Ia"].iloc[0]
    except (KeyError, IndexError):
        ia_prob = 0.0
        
    return probs.idxmax(), probs.max(), ia_prob


def run_superphot(ztf_id):
    """
    Run the complete Superphot Plus analysis pipeline for a given transient.

    This function performs the following steps:
    1. Fetches photometry data from MongoDB for ZTF and optionally LSST
    2. Processes and combines multi-survey photometry
    3. Applies extinction correction and phase calculation
    4. Fits light curves using Superphot Plus SVI sampler
    5. Classifies the transient using pre-trained LightGBM models
    6. Generates and saves a diagnostic plot

    Args:
        ztf_id (str): ZTF identifier for the transient to analyze.

    Returns:
        None: Results are saved to disk and printed. Returns early if insufficient data.

    Side Effects:
        - Saves a diagnostic plot to 'superphot_results/{ztf_id}_superphot.png'
        - Prints diagnostic information and photometry DataFrame
        - May print error messages if data is insufficient or processing fails
    """
    # Fetch ZTF photometry
    cand_info = fetch_mongo("ZTF_alerts_aux").find_one({"_id": str(ztf_id)})
    df_ztf = process_photometry(cand_info, "ZTF")
    
    # Attempt to fetch and combine LSST photometry if available
    if len(cand_info["aliases"]["LSST"]) != 0:
        lsst_id = cand_info["aliases"]["LSST"][0]
        lsst_cand_info = fetch_mongo("LSST_alerts_aux").find_one({"_id": str(lsst_id)})
        df_lsst = process_photometry(lsst_cand_info, "LSST")

        # Only include LSST data if it has sufficient coverage in both filters
        if (len(df_lsst.loc[df_lsst["filter"] == "r"]) >= 2 and
            len(df_lsst.loc[df_lsst["filter"] == "g"]) >= 2):
            df_final = pd.concat([df_ztf, df_lsst])
        else:
            df_final = df_ztf
    else:
        df_final = df_ztf

    # Filter to only r and g bands
    df_final = df_final.loc[df_final['filter'].isin(["r", "g"])]
    df_final.reset_index(drop=True, inplace=True)

    # Check for minimum data requirements
    if (len(df_final.loc[df_final["filter"] == "r"]) <= 2 or
        len(df_final.loc[df_final["filter"] == "g"]) <= 2):
        print(f"Not Enough Points {ztf_id}")
        return None

    # Add filter metadata for SNAPI
    df_final['filt_center'] = np.where(df_final['filter'] == 'r', 6366.38, 4746.48)
    df_final['filt_width'] = np.where(df_final['filter'] == 'r', 1553.43, 1317.15)
    df_final['filter'] = np.where(df_final['filter'] == 'r', 'ZTF_r', 'ZTF_g')
    df_final['zeropoint'] = 23.90  # AB mag
    df_final['upperlimit'] = False

    # Create SNAPI photometry object
    phot = Photometry(df_final)

    # Merge close-time observations
    new_lcs = []
    for lc in phot.light_curves:
        lc.merge_close_times(inplace=True)
        new_lcs.append(lc)

    phot = Photometry.from_light_curves(new_lcs)
    
    # Phase and truncate light curves
    phot.phase(inplace=True)
    phot.truncate(min_t=-50., max_t=100.)

    # Apply Milky Way extinction correction
    phot.correct_extinction(
        coordinates=SkyCoord(ra=92.44 * u.deg, dec=35.7 * u.deg),
        inplace=True
    )
    
    redshift = np.nan

    # Normalize photometry
    phot.normalize(inplace=True)

    # Pad light curves to nearest power of 2 for model input
    padded_lcs = []
    orig_size = len(phot.detections)
    num_pad = int(2**np.ceil(np.log2(orig_size)))
    fill = {
        'phase': 1000.,
        'flux': 0.1,
        'flux_error': 1000.,
        'zeropoint': 23.90,
        'upper_limit': False
    }


    for lc in phot.light_curves:
        padded_lc = lc.pad(fill, num_pad - len(lc.detections))
        padded_lcs.append(padded_lc)
    padded_phot = Photometry.from_light_curves(padded_lcs)


    # Load priors and fit using SVI sampler
    priors = SuperphotPrior.load(f'{MODELS_PATH}/global_priors_hier_svi')
    random_seed = 42

    svi_sampler = SVISampler(
        priors=priors,
        num_iter=10_000,
        random_state=random_seed
    )

    # Avoid polluting output with fitting logs
    with open(os.devnull, "w") as f, redirect_stdout(f), redirect_stderr(f):
        svi_sampler.fit_photometry(padded_phot, orig_num_times=orig_size)
    res = svi_sampler.result

    # Store fit parameters
    event_dict = {}
    for param in res.fit_parameters.columns:
        event_dict[f'superphot_plus_{param}'] = res.fit_parameters[param].median()

    # Filter fits by quality score
    score_cutoff = 1.2
    if orig_size >= 6:
        valid_fits = res.fit_parameters[res.score <= score_cutoff]
    else:
        valid_fits = res.fit_parameters
    
    if valid_fits.empty:
        print(f"Empty fits for {ztf_id}")
        return None
    
    try:
        # Identify early-phase fits (all observations before piecewise transition)
        early_fit_mask = (valid_fits['gamma_ZTF_r'] + valid_fits['t_0_ZTF_r'] > 
                          np.max(phot.times))
    except UnboundLocalError:
        print(f"No valid returns {ztf_id}")
        return None

    # Convert fit parameters to uncorrelated Gaussian draws
    uncorr_fits = priors.reverse_transform(valid_fits)
    event_dict['name'] = ztf_id
    uncorr_fits.index = [event_dict['name']] * len(uncorr_fits)

    # Load classification models
    full_model_fn = f"{MODELS_PATH}/model_superphot_full.pt"
    early_model_fn = f"{MODELS_PATH}/model_superphot_early.pt"

    full_model = SuperphotLightGBM.load(full_model_fn)
    early_model = SuperphotLightGBM.load(early_model_fn)

    # Classify using appropriate model (early vs. full phase)
    if len(valid_fits[early_fit_mask]) > len(valid_fits[~early_fit_mask]):
        # Use early-phase classifier
        class_noz, prob_noz, ia_prob_noz = evaluate_cal_probs(early_model, uncorr_fits)
        event_dict['superphot_plus_classifier'] = 'early_lightgbm_02_2025'
    else:
        # Use full-phase classifier
        class_noz, prob_noz, ia_prob_noz = evaluate_cal_probs(full_model, uncorr_fits)
        event_dict['superphot_plus_classifier'] = 'full_lightgbm_02_2025'

    # Store classification results
    if ~np.isnan(redshift):
        event_dict['superphot_plus_class_without_redshift'] = class_noz
        event_dict['superphot_plus_prob_without_redshift'] = np.round(prob_noz, 3)
    else:
        event_dict['superphot_plus_class'] = class_noz
        event_dict['superphot_plus_prob'] = np.round(prob_noz, 3)
        event_dict['superphot_non_Ia_prob'] = 1. - np.round(ia_prob_noz, 3)

    event_dict['superphot_plus_classified'] = True

    if event_dict['superphot_plus_prob'] > 0.5:
        # Generate diagnostic plot
        fig, ax = plt.subplots(figsize=(8, 6))
        formatter = Formatter()
        ax = svi_sampler.plot_fit(ax, formatter, phot)
        phot.plot(ax, formatter, mags=False)
        formatter.add_legend(ax)
        formatter.make_plot_pretty(ax)
        ax.set_xlabel('Phase', fontsize=15)
        ax.set_ylabel('Flux', fontsize=15)
        ax.tick_params(axis='both', which='major', labelsize=15)
        ax.legend()
        plt.title(
            f"{ztf_id}, Class: {event_dict['superphot_plus_class']}, "
            f"Probability: {event_dict['superphot_plus_prob']}",
            fontsize=18
        )
        plt.savefig(f"superphot_results/{ztf_id}_superphot.png")

    return None

if __name__ == "__main__":
    # --- CLI arguments ---
    parser = argparse.ArgumentParser(
        description=(
            "Run Superphot Plus on a specified object ID from Boom's MongoDB."
        )
    )
    parser.add_argument(
        "--object-id",
        type=str,
        default="ZTF18abwvfof",
        help="The object ID to run Superphot Plus on."
    )
    args = parser.parse_args()
    run_superphot(args.object_id)