import functools
import time
import requests

from logger import log, RED, YELLOW, GREEN, ENDC

SLOW_RESPONSE_THRESHOLD = 5  # seconds

def handle_timeout(method):
    """
    Decorator to handle requests timeouts and log slow responses.
    If a request takes longer than 5 seconds, log a warning.
    If a request times out, raise a TimeoutError with a custom message.
    """
    def get_request_type(method_name, args):
        """Return the method name or endpoint being called if method is 'api'"""
        if method_name == "api" and len(args) > 1:
            return args[1] # endpoint
        return method_name

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            start = time.time()
            result = method(self, *args, **kwargs)

            latency = time.time() - start
            if latency > SLOW_RESPONSE_THRESHOLD:
                log(f"{YELLOW}Warning - SkyPortal API is responding slowly to {get_request_type(method.__name__, args)} requests: {latency:.2f} seconds{ENDC}")

            return result
        except requests.exceptions.Timeout:
            raise TimeoutError(
                f"{RED}Timeout error{ENDC} - SkyPortal API not responding to {YELLOW}{get_request_type(method.__name__, args)}{ENDC} request"
            )
        except Exception as e:
            raise Exception(f"{RED}Error in {get_request_type(method.__name__, args)}{ENDC} - {e}")
    return wrapper

class SkyPortal:
    """
    SkyPortal API client

    Parameters
    ----------
    port : int
        Port to use
    token : str
        SkyPortal API token
    validate : bool, optional
        If True, validate the SkyPortal instance and token
    
    Attributes
    ----------
    base_url : str
        Base URL of the SkyPortal instance
    headers : dict
        Authorization headers to use
    """

    def __init__(self, instance, token, port=443, validate=True):
        # build the base URL from the instance and port
        self.base_url = f'{instance}'
        if port not in ['None', '', 80, 443]:
            self.base_url += f':{port}'
        
        self.headers = {'Authorization': f'token {token}'}

        # ping it to make sure it's up, if validate is True
        if validate:
            if not self.ping():
                raise ValueError('SkyPortal API not available')
            
            if not self.auth():
                raise ValueError('SkyPortal API authentication failed. Token may be invalid.')

    @handle_timeout
    def ping(self):
        """
        Check if the SkyPortal API is available

        Returns
        -------
        bool
            True if the API is available, False otherwise
        """
        response = requests.get(f"{self.base_url}/api/sysinfo", timeout=40)
        return response.status_code == 200

    @handle_timeout
    def auth(self):
        """
        Check if the SkyPortal Token provided is valid

        Returns
        -------
        bool
            True if the token is valid, False otherwise
        """
        response = requests.get(
            f"{self.base_url}/api/config",
            headers=self.headers,
            timeout=40
        )
        return response.status_code == 200

    @handle_timeout
    def api(self, method: str, endpoint: str, data=None, return_response=False):
        """
        Make an API request to SkyPortal

        Parameters
        ----------
        method : str
            HTTP method to use (GET, POST, PUT, PATCH, DELETE)
        endpoint : str
            API endpoint to query
        data : dict, optional
            JSON data to send with the request, as parameters or payload
        return_response : bool, optional
            If True, return the raw response instead of parsing JSON

        Returns
        -------
        requests.Response or dict
            If `return_response` is True, returns the raw `requests.Response` object.
            Otherwise, returns the parsed JSON response as a dictionary.
        """
        endpoint = f'{self.base_url}/{endpoint.strip("/")}'
        if method == 'GET':
            response = requests.request(method, endpoint, params=data, headers=self.headers, timeout=40)
        else:
            response = requests.request(method, endpoint, json=data, headers=self.headers)

        if return_response:
            return response

        try:
            body = response.json()
        except Exception:
            if "server error" in response.text.lower():
                raise ValueError('Server error.')
            raise ValueError(response.text)

        if response.status_code != 200:
            raise ValueError(body.get("message", response.text))

        return body.get('data')

    def fetch_all_pages(self, endpoint, payload, item_key):
        """
        Fetch all pages of a paginated API endpoint

        Returns
        -------
        list
            All items from all pages
        """
        items = []
        payload["pageNumber"] = 1
        payload["numPerPage"] = 1000,
        while True:
            results = self.api("GET", endpoint, data=payload)
            items += results[item_key]
            if results["totalMatches"] <= len(items):
                break
            payload["pageNumber"] += 1
            time.sleep(0.3)
        return items

    def save_to_groups(self, object_id, group_id):
        """
        Save an object to multiple groups

        Parameters
        ----------
        object_id : str
            ID of the object to save
        group_id : int
            ID of the group to save the object to
        Returns
        -------
        dict
            The decoded JSON response from the API

        """
        response = self.api("POST", f"/api/source_groups", data={
            "objId": object_id,
            "inviteGroupIds": [group_id]
        }, return_response=True).json()
        if response.get("status") == "success":
            log(f"{GREEN}Object {object_id} saved.{ENDC}")
        elif response.get("message").startswith("Source already saved"):
            log(f"{YELLOW}Object {object_id} already saved.{ENDC}")
        else:
            log(f"{RED}Error saving object {object_id}: {response.get('message')}{ENDC}")
        return response

    def add_annotation(self, object_id, group_ids, origin, data):
        """
        Add an annotation to a source

        Parameters
        ----------
        object_id : str
            ID of the object to annotate
        group_ids : list of int
            List of group IDs that can view the annotation
        origin : str
            Origin of the annotation
        data : dict
            Annotation data

        Returns
        -------
        dict
            The decoded JSON response from the API
        """
        response = self.api("POST", f"/api/sources/{object_id}/annotations", data={
            "group_ids": group_ids,
            "origin": origin,
            "data": data,
        }, return_response=True).json()
        if response.get("status") == "success":
            log(f"{GREEN}Annotation added to {object_id}.{ENDC}")
        elif response.get("message").startswith("Annotation already exists"):
            log(f"{YELLOW}Annotation already exists for {object_id}.{ENDC}")
        else:
            log(f"{RED}Error adding annotation to {object_id}: {response.get('message')}{ENDC}")
        return response