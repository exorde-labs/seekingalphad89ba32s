"""
In this script we are going to collect data from Seeking Alpha. We will navigate to this link:

https://seekingalpha.com/market-news

Once on it, we can extract all the latest news posts.

A simple GET request will return the page. We can then perform a lookup for all the elements following this structure:

<div class="km-IL"> :: the card class that encompasses what we want
    ...
    <a data-test-id="post-list-item-title" href=[link to news]/> :: the subclass that encompasses the link to the post
    ...
</div>

With this, we can extract the links to every news post. They are ordered by post date, so once we reach a news post that
is outside of our time window, we can exit early.

Another GET request on the identified links of interest will yield the relevant posts and their contents.

Once the GET request returns on the link of the post, look for these elements:

<h1 data-test-id="post-title"/> ::  returns the title of the post
<span data-test-id="post-date"/> :: returns the date of the post in this format: "Jul. 18, 2023 8:13 AM ET"
<span data-test-id="post-author-nick"/> :: returns the author's name
<div data-test-id="content-container"/> :: the content of the post
    <p/> :: look for all directly-related p objects within the container to get the content (otherwise you'll get other things too)

"""
import time
import re
import requests
import random
from bs4 import BeautifulSoup
from typing import AsyncGenerator
from datetime import datetime, timedelta
import pytz
from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    Title,
    Url,
    Domain,
)
import logging

# GLOBAL VARIABLES
USER_AGENT_LIST = [
    'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'
]
DEFAULT_OLDNESS_SECONDS = 360
DEFAULT_MAXIMUM_ITEMS = 25
DEFAULT_MIN_POST_LENGTH = 10
REGEX_PATTERN = r"^More on \w+(:)?$"


def request_content_with_timeout(_url, _max_age):
    """
    Returns all relevant information from the news post
    :param _max_age: the maximum age we will allow for the post in seconds
    :param _url: the url of the post
    :return: the content of the post

    <h1 data-test-id="post-title"/> ::  returns the title of the post
    <span data-test-id="post-date"/> :: returns the date of the post in this format: "Jul. 18, 2023 8:13 AM ET"
    <span data-test-id="post-author-nick"/> :: returns the author's name
    <div data-test-id="content-container"/> :: the content of the post
        <p/> :: look for all directly-related p objects within the container to get the content (otherwise you'll get other things too)
    """
    try:
        response = requests.get(_url, headers={'User-Agent': random.choice(USER_AGENT_LIST)}, timeout=8.0)
        soup = BeautifulSoup(response.text, 'html.parser')

        post_date = convert_date_to_standard_format(soup.find("span", {"data-test-id": "post-date"}).text)
        if not check_for_max_age(post_date, _max_age):
            return None

        post_title = soup.find("h1", {"data-test-id": "post-title"}).text
        author = soup.find("span", {"data-test-id": "post-author-nick"}).text.lstrip("By: ")

        content_container = soup.find("div", {"data-test-id": "content-container"})

        content_paragraphs = content_container.findChildren(recursive=False)  # get all children

        content = ""

        for el in content_paragraphs:
            if el.name == "h2":
                break  # end of the valuable content within the post
            if not el.name == "figure" and el.text:
                if re.match(REGEX_PATTERN, el.text):
                    # make sure we remove the last bit "More on..." that sometimes stays
                    break
                content += el.text
                content += "\n"

        return Item(
            title=Title(post_title),
            content=Content(content),
            author=Author(author),
            created_at=CreatedAt(post_date),
            url=Url(_url),
            domain=Domain("seekingalpha.com"))
    except Exception as e:
        logging.exception("[Seekingalpha] Error : {e}")


async def request_entries_with_timeout(_url, _max_age):
    """
    Extracts all card elements from the latest news section
    :param _max_age: the maximum age we will allow for the post in seconds
    :param _url: the url where we will find the latest posts
    :return: the card elements from which we can extract the relevant information
    """
    try:
        response = requests.get(_url, headers={'User-Agent': random.choice(USER_AGENT_LIST)}, timeout=8.0)
        soup = BeautifulSoup(response.text, 'html.parser')
        entries = soup.find_all("a", {"data-test-id": "post-list-item-title"})
        async for item in parse_entry_for_elements(entries, _max_age):
            yield item
    except Exception as e:
        logging.exception("[Seekingalpha] Error : {e}")


def convert_date_to_standard_format(_date):

    # date contains 6 elements [Month (abbreviated), Day Number, Year, Time, AM/PM, Timezone ET]
    # ET zone is 4 hours back from UTC + 0, we need to add 4 hours to this date time

    # Remove the "ET" part from the input time string
    cleaned_time = _date.replace(" ET", "")

    # Convert the input time string to a datetime object
    datetime_obj = datetime.strptime(cleaned_time, "%b. %d, %Y %I:%M %p") + timedelta(hours=4)

    # Convert the datetime object to the desired output format
    return datetime_obj.strftime("%Y-%m-%dT%H:%M:%S.00Z")


def check_for_max_age(_date, _max_age):
    """
    Checks if the entry is within the max age bracket that we are looking for
    :param _date: the datetime from the entry
    :param _max_age: the max age to which we will be comparing the timestamp
    :return: true if it is within the age bracket, false otherwise
    """
    date_to_check = datetime.strptime(_date, "%Y-%m-%dT%H:%M:%S.00Z")
    now_time = datetime.strptime(datetime.strftime(datetime.now(pytz.utc), "%Y-%m-%dT%H:%M:%S.00Z"), "%Y-%m-%dT%H:%M:%S.00Z")

    if (now_time - date_to_check).total_seconds() <= _max_age:
        return True
    else:
        return False


async def parse_entry_for_elements(_cards, _max_age):
    """
    Parses every card element to find the information we want
    :param _max_age: The maximum age we will allow for the post in seconds
    :param _cards: The parent card objects from which we will be gathering the information
    :return: All the parameters we need to return an Item instance
    """
    try:
        for card in _cards:
            item = request_content_with_timeout("https://seekingalpha.com" + card["href"], _max_age)
            if item:
                yield item
            else:
                break  # if this item was not in the time bracket that interests us, the following ones will not be either
    except Exception as e:
        logging.exception("[Seekingalpha] Error : {e}")


def read_parameters(parameters):
    # Check if parameters is not empty or None
    if parameters and isinstance(parameters, dict):
        try:
            max_oldness_seconds = parameters.get("max_oldness_seconds", DEFAULT_OLDNESS_SECONDS)
        except KeyError:
            max_oldness_seconds = DEFAULT_OLDNESS_SECONDS

        try:
            maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)
        except KeyError:
            maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS

        try:
            min_post_length = parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
        except KeyError:
            min_post_length = DEFAULT_MIN_POST_LENGTH

    else:
        # Assign default values if parameters is empty or None
        max_oldness_seconds = DEFAULT_OLDNESS_SECONDS
        maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS
        min_post_length = DEFAULT_MIN_POST_LENGTH

    return max_oldness_seconds, maximum_items_to_collect, min_post_length


async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    url_main_endpoint = "https://seekingalpha.com/market-news"
    yielded_items = 0
    max_oldness_seconds, maximum_items_to_collect, min_post_length = read_parameters(parameters)
    logging.info(f"[Seeking Alpha] - Scraping items posted less than {max_oldness_seconds} seconds ago.")

    async for item in request_entries_with_timeout(url_main_endpoint, max_oldness_seconds):
        yielded_items += 1
        yield item
        logging.info(f"[Seeking Alpha] Found new post :\t {item.title}, posted at { item.created_at}, URL = {item.url}" )
        if yielded_items >= maximum_items_to_collect:
            break